# NetMessenger Design Diary - IT23847026

## 1. Concurrency Model Decision
- **Decision:** Selected POSIX Threads (`pthread`) with per-client thread allocation and a central mutex (`clients_mutex` and `rooms_mutex`).
- **Rationale:** Compared to `select`/`poll`, dedicated worker threads allow blocking line-reading and binary file streaming (`SENDFILE`) without stalling other active clients or maintaining a complex finite state machine for partial file chunks.

## 2. Protocol Framing & Buffer Management
- **Obstacle:** TCP is a continuous byte stream; multiple commands could arrive in a single `recv()` call, or a command could be fragmented across reads.
- **Solution:** Implemented `read_line()` to consume character-by-character until `\n`. For binary file transfer (`SENDFILE`), the server reads text metadata up to `\n`, switches immediately into binary transfer mode, and loops `recv()` until exactly `<filesize>` bytes are written to disk.

## 3. Storage Hierarchy & Personalisation
- **Design:** Configured listening port 13026 ($6000 + 7026$) and Node ID tag `NID:8470`. Implemented recursive directory creation under `./storage/IT23847026/<sender>/<filename>`.

