#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <libgen.h>

#define DEFAULT_PORT 13026
#define BUFFER_SIZE 4096

int sockfd = -1;
volatile int running = 1;

// Helper: Read a single line up to '\n'
ssize_t read_line(int sock, char *buffer, size_t max_len) {
    size_t total = 0;
    char ch;
    while (total < max_len - 1) {
        ssize_t n = recv(sock, &ch, 1, 0);
        if (n <= 0) {
            if (n == 0 && total == 0) return 0;
            break;
        }
        if (ch == '\r') continue;
        if (ch == '\n') break;
        buffer[total++] = ch;
    }
    buffer[total] = '\0';
    return total;
}

// Background thread: Listen for incoming messages/files from server
void *receive_handler(void *arg) {
    (void)arg;
    char buffer[BUFFER_SIZE];

    while (running) {
        ssize_t n = read_line(sockfd, buffer, sizeof(buffer));
        if (n <= 0) {
            if (running) {
                printf("\n[Disconnected from server]\n");
                running = 0;
            }
            break;
        }

        // Handle incoming forwarded file: FILE <sender> <filename> <filesize>
        if (strncmp(buffer, "FILE ", 5) == 0) {
            char sender[32];
            char filename[64];
            long filesize = 0;

            if (sscanf(buffer + 5, "%31s %63s %ld", sender, filename, &filesize) == 3) {
                printf("\n[Receiving file '%s' (%ld bytes) from %s...]\n", filename, filesize, sender);
                
                char local_name[128];
                snprintf(local_name, sizeof(local_name), "received_%s", filename);
                FILE *fp = fopen(local_name, "wb");

                long remaining = filesize;
                char stream_buf[BUFFER_SIZE];
                while (remaining > 0) {
                    size_t chunk = ((size_t)remaining > sizeof(stream_buf)) ? sizeof(stream_buf) : (size_t)remaining;
                    ssize_t bytes_recv = recv(sockfd, stream_buf, chunk, 0);
                    if (bytes_recv <= 0) break;
                    if (fp) fwrite(stream_buf, 1, bytes_recv, fp);
                    remaining -= bytes_recv;
                }

                if (fp) {
                    fclose(fp);
                    printf("[File saved locally as '%s']\n> ", local_name);
                    fflush(stdout);
                }
                continue;
            }
        }

        // Regular response/message output
        printf("%s\n> ", buffer);
        fflush(stdout);
    }
    return NULL;
}

// Helper function to send files to the server
void handle_sendfile_command(const char *cmd_line) {
    char target[32];
    char filepath[256];

    if (sscanf(cmd_line + 9, "%31s %255s", target, filepath) < 2) {
        printf("Usage: SENDFILE <target> <filepath>\n> ");
        fflush(stdout);
        return;
    }

    FILE *fp = fopen(filepath, "rb");
    if (!fp) {
        printf("Error: Cannot open local file '%s'\n> ", filepath);
        fflush(stdout);
        return;
    }

    // Determine file size
    fseek(fp, 0, SEEK_END);
    long filesize = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    if (filesize > 10 * 1024 * 1024) {
        printf("Error: File exceeds 10MB limit\n> ");
        fclose(fp);
        fflush(stdout);
        return;
    }

    // Extract bare filename from path
    char path_copy[256];
    strncpy(path_copy, filepath, sizeof(path_copy) - 1);
    char *fname = basename(path_copy);

    // Send header line
    char header[512];
    snprintf(header, sizeof(header), "SENDFILE %s %s %ld\n", target, fname, filesize);
    send(sockfd, header, strlen(header), 0);

    // Send raw bytes immediately
    char send_buf[BUFFER_SIZE];
    size_t read_bytes;
    while ((read_bytes = fread(send_buf, 1, sizeof(send_buf), fp)) > 0) {
        send(sockfd, send_buf, read_bytes, 0);
    }
    fclose(fp);
}

int main(int argc, char *argv[]) {
    char *server_ip = "127.0.0.1";
    int port = DEFAULT_PORT;

    if (argc >= 2) server_ip = argv[1];
    if (argc >= 3) port = atoi(argv[2]);

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, server_ip, &serv_addr.sin_addr) <= 0) {
        perror("Invalid server IP");
        exit(EXIT_FAILURE);
    }

    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("Connection failed");
        exit(EXIT_FAILURE);
    }

    printf("Connected to NetMessenger server at %s:%d\n", server_ip, port);
    printf("Type commands (REGISTER, LIST, BCAST, PMSG, JOIN, LEAVE, ROOMS, RMSG, SENDFILE, QUIT)\n> ");
    fflush(stdout);

    pthread_t recv_tid;
    if (pthread_create(&recv_tid, NULL, receive_handler, NULL) != 0) {
        perror("Failed to create receiver thread");
        close(sockfd);
        exit(EXIT_FAILURE);
    }

    char line[BUFFER_SIZE];
    while (running && fgets(line, sizeof(line), stdin)) {
        // Strip trailing newline character for parsing
        line[strcspn(line, "\r\n")] = 0;
        if (strlen(line) == 0) {
            printf("> ");
            fflush(stdout);
            continue;
        }

        if (strncmp(line, "SENDFILE ", 9) == 0) {
            handle_sendfile_command(line);
        } else {
            // Re-append \n for standard protocol commands
            char out[BUFFER_SIZE + 2];
            snprintf(out, sizeof(out), "%s\n", line);
            send(sockfd, out, strlen(out), 0);

            if (strcmp(line, "QUIT") == 0) {
                running = 0;
                break;
            }
        }
    }

    pthread_cancel(recv_tid);
    pthread_join(recv_tid, NULL);
    close(sockfd);
    printf("Client exited cleanly.\n");
    return 0;
}
