#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define PORT 13026
#define NID "NID:8470"
#define LOG_FILE "netmsg_IT23847026.log"
#define STORAGE_BASE "./storage/IT23847026"
#define BUFFER_SIZE 4096
#define MSG_BUFFER_SIZE 8192
#define MAX_CLIENTS 64
#define MAX_ROOMS 32
#define MAX_ROOM_MEMBERS 64

typedef struct {
    int sockfd;
    struct sockaddr_in address;
    char username[32];
    int registered;
} client_t;

typedef struct {
    char name[32];
    char members[MAX_ROOM_MEMBERS][32];
    int member_count;
} room_t;

client_t *clients[MAX_CLIENTS];
room_t rooms[MAX_ROOMS];
int room_count = 0;

pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t rooms_mutex = PTHREAD_MUTEX_INITIALIZER;

// Helper: Logging with timestamps to file and stdout
void log_event(const char *event) {
    FILE *fp = fopen(LOG_FILE, "a");
    time_t now = time(NULL);
    char tbuf[64];
    strftime(tbuf, sizeof(tbuf), "%Y-%m-%d %H:%M:%S", localtime(&now));
    
    if (fp) {
        fprintf(fp, "[%s] %s\n", tbuf, event);
        fclose(fp);
    }
    printf("[%s] %s\n", tbuf, event);
}

// Helper: Line framing reading single character up to '\n'
ssize_t read_line(int sock, char *buffer, size_t max_len) {
    size_t total = 0;
    char ch;
    while (total < max_len - 1) {
        ssize_t n = recv(sock, &ch, 1, 0);
        if (n <= 0) {
            if (n == 0 && total == 0) return 0; // Disconnected cleanly
            break;
        }
        if (ch == '\r') continue;
        if (ch == '\n') break;
        buffer[total++] = ch;
    }
    buffer[total] = '\0';
    return total;
}

// Helper: Append NID tag to OK and ERR control responses
void send_response(int sock, const char *prefix, const char *body) {
    char resp[MSG_BUFFER_SIZE];
    if (body && strlen(body) > 0) {
        snprintf(resp, sizeof(resp), "%s %s %s\n", prefix, body, NID);
    } else {
        snprintf(resp, sizeof(resp), "%s %s\n", prefix, NID);
    }
    send(sock, resp, strlen(resp), 0);
}

// Helper: Forward message lines without NID tag
void send_raw(int sock, const char *raw_msg) {
    send(sock, raw_msg, strlen(raw_msg), 0);
}

// Helper: Recursive directory creation (like mkdir -p)
void create_dir_if_not_exists(const char *path) {
    char tmp[512];
    char *p = NULL;
    size_t len;

    snprintf(tmp, sizeof(tmp), "%s", path);
    len = strlen(tmp);
    if (tmp[len - 1] == '/') tmp[len - 1] = 0;
    for (p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = 0;
            mkdir(tmp, 0755);
            *p = '/';
        }
    }
    mkdir(tmp, 0755);
}

// Helper: Cleanly remove client from all rooms on disconnect
void remove_client_from_all_rooms(const char *username) {
    pthread_mutex_lock(&rooms_mutex);
    for (int i = 0; i < room_count; i++) {
        for (int j = 0; j < rooms[i].member_count; j++) {
            if (strcmp(rooms[i].members[j], username) == 0) {
                for (int k = j; k < rooms[i].member_count - 1; k++) {
                    strcpy(rooms[i].members[k], rooms[i].members[k + 1]);
                }
                rooms[i].member_count--;
                break;
            }
        }
    }
    pthread_mutex_unlock(&rooms_mutex);
}

void *handle_client(void *arg) {
    client_t *cli = (client_t *)arg;
    char buffer[BUFFER_SIZE];
    char log_buf[512];

    snprintf(log_buf, sizeof(log_buf), "New connection from %s:%d",
             inet_ntoa(cli->address.sin_addr), ntohs(cli->address.sin_port));
    log_event(log_buf);

    while (1) {
        ssize_t bytes_read = read_line(cli->sockfd, buffer, sizeof(buffer));
        if (bytes_read <= 0) break; // Client disconnected or error

        // 1. REGISTER <username>
        if (strncmp(buffer, "REGISTER ", 9) == 0) {
            char uname[32];
            sscanf(buffer + 9, "%31s", uname);

            if (cli->registered) {
                send_response(cli->sockfd, "ERR 005", "ALREADY_REGISTERED");
                continue;
            }

            pthread_mutex_lock(&clients_mutex);
            int taken = 0;
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i] && clients[i]->registered && strcmp(clients[i]->username, uname) == 0) {
                    taken = 1;
                    break;
                }
            }

            if (taken) {
                pthread_mutex_unlock(&clients_mutex);
                send_response(cli->sockfd, "ERR 001", "USERNAME_TAKEN");
            } else {
                strncpy(cli->username, uname, sizeof(cli->username) - 1);
                cli->registered = 1;

                char notify[128];
                snprintf(notify, sizeof(notify), "MSG BCAST SERVER User %s joined\n", cli->username);
                for (int i = 0; i < MAX_CLIENTS; i++) {
                    if (clients[i] && clients[i]->registered && clients[i]->sockfd != cli->sockfd) {
                        send_raw(clients[i]->sockfd, notify);
                    }
                }
                pthread_mutex_unlock(&clients_mutex);

                char ok_body[64];
                snprintf(ok_body, sizeof(ok_body), "REGISTERED %s", cli->username);
                send_response(cli->sockfd, "OK", ok_body);

                snprintf(log_buf, sizeof(log_buf), "User registered: %s", cli->username);
                log_event(log_buf);
            }
        }
        // Verification: Must be registered first
        else if (!cli->registered) {
            send_response(cli->sockfd, "ERR 000", "NOT_REGISTERED");
        }
        // 2. LIST
        else if (strcmp(buffer, "LIST") == 0) {
            char user_list[BUFFER_SIZE] = "USERS ";
            pthread_mutex_lock(&clients_mutex);
            int first = 1;
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i] && clients[i]->registered) {
                    if (!first) strcat(user_list, ",");
                    strcat(user_list, clients[i]->username);
                    first = 0;
                }
            }
            pthread_mutex_unlock(&clients_mutex);
            send_response(cli->sockfd, "OK", user_list);
        }
        // 3. BCAST <message>
        else if (strncmp(buffer, "BCAST ", 6) == 0) {
            char *msg = buffer + 6;
            char forward[MSG_BUFFER_SIZE];
            snprintf(forward, sizeof(forward), "MSG BCAST %s %s\n", cli->username, msg);

            pthread_mutex_lock(&clients_mutex);
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i] && clients[i]->registered && clients[i]->sockfd != cli->sockfd) {
                    send_raw(clients[i]->sockfd, forward);
                }
            }
            pthread_mutex_unlock(&clients_mutex);

            send_response(cli->sockfd, "OK", "SENT");

            snprintf(log_buf, sizeof(log_buf), "Broadcast message from %s", cli->username);
            log_event(log_buf);
        }
        // 4. PMSG <username> <message>
        else if (strncmp(buffer, "PMSG ", 5) == 0) {
            char target[32];
            char msg[BUFFER_SIZE];
            if (sscanf(buffer + 5, "%31s %[^\n]", target, msg) < 2) {
                send_response(cli->sockfd, "ERR 006", "INVALID_FORMAT");
                continue;
            }

            int found = 0;
            pthread_mutex_lock(&clients_mutex);
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i] && clients[i]->registered && strcmp(clients[i]->username, target) == 0) {
                    char forward[MSG_BUFFER_SIZE];
                    snprintf(forward, sizeof(forward), "MSG PRIV %s %s\n", cli->username, msg);
                    send_raw(clients[i]->sockfd, forward);
                    found = 1;
                    break;
                }
            }
            pthread_mutex_unlock(&clients_mutex);

            if (found) {
                send_response(cli->sockfd, "OK", "SENT");
                snprintf(log_buf, sizeof(log_buf), "Private message from %s to %s", cli->username, target);
                log_event(log_buf);
            } else {
                send_response(cli->sockfd, "ERR 002", "USER_NOT_FOUND");
            }
        }
        // 5. JOIN <room>
        else if (strncmp(buffer, "JOIN ", 5) == 0) {
            char rname[32];
            sscanf(buffer + 5, "%31s", rname);

            pthread_mutex_lock(&rooms_mutex);
            int idx = -1;
            for (int i = 0; i < room_count; i++) {
                if (strcmp(rooms[i].name, rname) == 0) {
                    idx = i;
                    break;
                }
            }

            if (idx == -1 && room_count < MAX_ROOMS) {
                idx = room_count++;
                strncpy(rooms[idx].name, rname, 31);
                rooms[idx].member_count = 0;
            }

            if (idx != -1) {
                int already_in = 0;
                for (int j = 0; j < rooms[idx].member_count; j++) {
                    if (strcmp(rooms[idx].members[j], cli->username) == 0) {
                        already_in = 1;
                        break;
                    }
                }
                if (!already_in && rooms[idx].member_count < MAX_ROOM_MEMBERS) {
                    strcpy(rooms[idx].members[rooms[idx].member_count++], cli->username);
                }
                pthread_mutex_unlock(&rooms_mutex);

                char ok_body[64];
                snprintf(ok_body, sizeof(ok_body), "JOINED %s", rname);
                send_response(cli->sockfd, "OK", ok_body);
            } else {
                pthread_mutex_unlock(&rooms_mutex);
                send_response(cli->sockfd, "ERR 007", "MAX_ROOMS_REACHED");
            }
        }
        // 6. LEAVE <room>
        else if (strncmp(buffer, "LEAVE ", 6) == 0) {
            char rname[32];
            sscanf(buffer + 6, "%31s", rname);

            pthread_mutex_lock(&rooms_mutex);
            int found_room = 0;
            for (int i = 0; i < room_count; i++) {
                if (strcmp(rooms[i].name, rname) == 0) {
                    found_room = 1;
                    for (int j = 0; j < rooms[i].member_count; j++) {
                        if (strcmp(rooms[i].members[j], cli->username) == 0) {
                            for (int k = j; k < rooms[i].member_count - 1; k++) {
                                strcpy(rooms[i].members[k], rooms[i].members[k + 1]);
                            }
                            rooms[i].member_count--;
                            break;
                        }
                    }
                    break;
                }
            }
            pthread_mutex_unlock(&rooms_mutex);

            if (found_room) {
                char ok_body[64];
                snprintf(ok_body, sizeof(ok_body), "LEFT %s", rname);
                send_response(cli->sockfd, "OK", ok_body);
            } else {
                send_response(cli->sockfd, "ERR 003", "ROOM_NOT_FOUND");
            }
        }
        // 7. ROOMS
        else if (strcmp(buffer, "ROOMS") == 0) {
            char rlist[BUFFER_SIZE] = "ROOMS ";
            pthread_mutex_lock(&rooms_mutex);
            int first = 1;
            for (int i = 0; i < room_count; i++) {
                if (!first) strcat(rlist, ",");
                strcat(rlist, rooms[i].name);
                first = 0;
            }
            pthread_mutex_unlock(&rooms_mutex);
            send_response(cli->sockfd, "OK", rlist);
        }
        // 8. RMSG <room> <message>
        else if (strncmp(buffer, "RMSG ", 5) == 0) {
            char rname[32];
            char msg[BUFFER_SIZE];
            if (sscanf(buffer + 5, "%31s %[^\n]", rname, msg) < 2) {
                send_response(cli->sockfd, "ERR 006", "INVALID_FORMAT");
                continue;
            }

            pthread_mutex_lock(&rooms_mutex);
            int r_idx = -1;
            for (int i = 0; i < room_count; i++) {
                if (strcmp(rooms[i].name, rname) == 0) {
                    r_idx = i;
                    break;
                }
            }

            if (r_idx == -1) {
                pthread_mutex_unlock(&rooms_mutex);
                send_response(cli->sockfd, "ERR 003", "ROOM_NOT_FOUND");
            } else {
                char forward[MSG_BUFFER_SIZE];
                snprintf(forward, sizeof(forward), "MSG ROOM %s %s %s\n", rname, cli->username, msg);

                pthread_mutex_lock(&clients_mutex);
                for (int m = 0; m < rooms[r_idx].member_count; m++) {
                    const char *member_name = rooms[r_idx].members[m];
                    if (strcmp(member_name, cli->username) == 0) continue;

                    for (int c = 0; c < MAX_CLIENTS; c++) {
                        if (clients[c] && clients[c]->registered && strcmp(clients[c]->username, member_name) == 0) {
                            send_raw(clients[c]->sockfd, forward);
                            break;
                        }
                    }
                }
                pthread_mutex_unlock(&clients_mutex);
                pthread_mutex_unlock(&rooms_mutex);

                send_response(cli->sockfd, "OK", "SENT");
            }
        }
        // 9. SENDFILE <target> <filename> <filesize>
        else if (strncmp(buffer, "SENDFILE ", 9) == 0) {
            char target[32];
            char filename[64];
            long filesize = 0;

            if (sscanf(buffer + 9, "%31s %63s %ld", target, filename, &filesize) < 3 || filesize <= 0) {
                send_response(cli->sockfd, "ERR 006", "INVALID_FORMAT");
                continue;
            }

            // Reject files exceeding 10MB
            if (filesize > 10 * 1024 * 1024) {
                send_response(cli->sockfd, "ERR 004", "FILE_TOO_LARGE");
                continue;
            }

            int is_room = 0;
            int target_client_fd = -1;

            pthread_mutex_lock(&rooms_mutex);
            for (int r = 0; r < room_count; r++) {
                if (strcmp(rooms[r].name, target) == 0) {
                    is_room = 1;
                    break;
                }
            }
            pthread_mutex_unlock(&rooms_mutex);

            if (!is_room) {
                pthread_mutex_lock(&clients_mutex);
                for (int c = 0; c < MAX_CLIENTS; c++) {
                    if (clients[c] && clients[c]->registered && strcmp(clients[c]->username, target) == 0) {
                        target_client_fd = clients[c]->sockfd;
                        break;
                    }
                }
                pthread_mutex_unlock(&clients_mutex);

                if (target_client_fd == -1) {
                    send_response(cli->sockfd, "ERR 002", "USER_NOT_FOUND");
                    continue;
                }
            }

            // Create personalised server directory: ./storage/IT23847026/<sender>/
            char user_storage_dir[256];
            snprintf(user_storage_dir, sizeof(user_storage_dir), "%s/%s", STORAGE_BASE, cli->username);
            create_dir_if_not_exists(user_storage_dir);

            char full_file_path[512];
            snprintf(full_file_path, sizeof(full_file_path), "%s/%s", user_storage_dir, filename);

            FILE *dest_fp = fopen(full_file_path, "wb");
            if (!dest_fp) {
                send_response(cli->sockfd, "ERR 009", "CANNOT_SAVE_FILE");
                continue;
            }

            char notify_header[256];
            snprintf(notify_header, sizeof(notify_header), "FILE %s %s %ld\n", cli->username, filename, filesize);

            if (!is_room && target_client_fd != -1) {
                send(target_client_fd, notify_header, strlen(notify_header), 0);
            }

            // Exact byte counting for raw stream
            long remaining = filesize;
            char stream_buf[BUFFER_SIZE];
            int transfer_error = 0;

            while (remaining > 0) {
                size_t chunk = ((size_t)remaining > sizeof(stream_buf)) ? sizeof(stream_buf) : (size_t)remaining;                
                ssize_t n = recv(cli->sockfd, stream_buf, chunk, 0);
                if (n <= 0) {
                    transfer_error = 1;
                    break;
                }
                fwrite(stream_buf, 1, n, dest_fp);

                if (!is_room && target_client_fd != -1) {
                    send(target_client_fd, stream_buf, n, 0);
                }
                remaining -= n;
            }

            fclose(dest_fp);

            if (transfer_error) {
                snprintf(log_buf, sizeof(log_buf), "File transfer aborted from %s", cli->username);
                log_event(log_buf);
                break;
            }

            char ok_body[128];
            snprintf(ok_body, sizeof(ok_body), "FILE RECEIVED %s", filename);
            send_response(cli->sockfd, "OK", ok_body);

            snprintf(log_buf, sizeof(log_buf), "File transferred: %s from %s to %s (%ld bytes)",
                     filename, cli->username, target, filesize);
            log_event(log_buf);
        }
        // 10. QUIT
        else if (strcmp(buffer, "QUIT") == 0) {
            send_response(cli->sockfd, "OK", "BYE");
            break;
        } else {
            send_response(cli->sockfd, "ERR 008", "UNKNOWN_COMMAND");
        }
    }

    close(cli->sockfd);

    if (cli->registered) {
        remove_client_from_all_rooms(cli->username);

        char leave_notify[128];
        snprintf(leave_notify, sizeof(leave_notify), "MSG BCAST SERVER User %s left\n", cli->username);

        pthread_mutex_lock(&clients_mutex);
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i] && clients[i]->registered && clients[i]->sockfd != cli->sockfd) {
                send_raw(clients[i]->sockfd, leave_notify);
            }
        }
        pthread_mutex_unlock(&clients_mutex);
    }

    pthread_mutex_lock(&clients_mutex);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i] == cli) {
            clients[i] = NULL;
            break;
        }
    }
    pthread_mutex_unlock(&clients_mutex);

    snprintf(log_buf, sizeof(log_buf), "Client disconnected: %s",
             cli->registered ? cli->username : "unregistered");
    log_event(log_buf);

    free(cli);
    return NULL;
}

int main() {
    int server_fd;
    struct sockaddr_in serv_addr;

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(PORT);

    if (bind(server_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("Bind failed");
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 10) < 0) {
        perror("Listen failed");
        exit(EXIT_FAILURE);
    }

    printf("Server listening on port %d (%s)...\n", PORT, NID);
    log_event("Server started successfully.");

    while (1) {
        struct sockaddr_in cli_addr;
        socklen_t cli_len = sizeof(cli_addr);
        int client_fd = accept(server_fd, (struct sockaddr *)&cli_addr, &cli_len);
        if (client_fd < 0) {
            perror("Accept failed");
            continue;
        }

        client_t *cli = (client_t *)malloc(sizeof(client_t));
        cli->sockfd = client_fd;
        cli->address = cli_addr;
        cli->registered = 0;
        cli->username[0] = '\0';

        pthread_mutex_lock(&clients_mutex);
        int added = 0;
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (!clients[i]) {
                clients[i] = cli;
                added = 1;
                break;
            }
        }
        pthread_mutex_unlock(&clients_mutex);

        if (added) {
            pthread_t tid;
            pthread_create(&tid, NULL, handle_client, (void *)cli);
            pthread_detach(tid);
        } else {
            send_response(client_fd, "ERR 500", "SERVER_BUSY");
            close(client_fd);
            free(cli);
        }
    }

    close(server_fd);
    return 0;
}
