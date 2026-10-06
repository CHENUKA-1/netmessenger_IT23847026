# Structured Reflection on AI Use and Learning - IT23847026

### 1. Which AI tools were used and at what stages?
I collaborated with Gemini during the initial planning, architecture selection, and debugging stages of the NetMessenger application. Specifically, AI assisted in clarifying the protocol framing rules, setting up the POSIX thread worker templates, and resolving compiler warnings during build automation.

### 2. What did the AI do well, and where did it mislead?
The AI was effective at outlining socket setup boilerplate (`socket`, `bind`, `listen`, `accept`) and generating clean string-handling routines for command parsing. However, it initially overlooked compiler warnings under strict `-Wall -Wextra` flags, specifically signed-versus-unsigned comparisons between `long` file sizes and `sizeof()` return types (`size_t`). It also required manual oversight to ensure that `MSG` forwarding lines were strictly separated from control responses so the `NID:8470` tag was not illegally appended to chat forwards.

### 3. What was changed, added, or rejected from AI suggestions?
I refined the proposed concurrency model to ensure thread safety across shared client arrays and chat room structures, replacing naive iterations with dedicated mutex locks (`pthread_mutex_lock`). I also restructured the binary file receiver to enforce a strict 10MB limit and implemented robust directory tree generation (`create_dir_if_not_exists`) to prevent storage write failures when subdirectories did not yet exist.

### 4. What was learned about network programming?
This assignment solidified my understanding of stream-oriented communication over TCP. Unlike UDP datagrams, TCP does not preserve message boundaries, requiring explicit application-level framing (`\n` line termination and byte-counted binary payloads). Managing concurrent clients highlighted the necessity of thread synchronization and race condition prevention when accessing shared data structures. Handling unexpected client terminations taught me the importance of detecting read zero-returns and broken pipes to gracefully reclaim server resources without crashing.


