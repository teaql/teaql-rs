#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/prctl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* Verification-only Linux supervisor. A process group alone cannot own a
 * compiler which creates another session. Adopt and reap our descendants;
 * never enumerate or signal processes belonging to another task. */
static volatile sig_atomic_t interrupted;
static void receive_signal(int value) { interrupted = value; }

static long long milliseconds(void) {
    struct timespec value;
    if (clock_gettime(CLOCK_MONOTONIC, &value) != 0) _exit(126);
    return (long long)value.tv_sec * 1000 + value.tv_nsec / 1000000;
}

static int signal_children(int value) {
    char name[128];
    snprintf(name, sizeof(name), "/proc/self/task/%ld/children", (long)getpid());
    FILE *input = fopen(name, "r");
    if (!input) return -1;
    long pid;
    while (fscanf(input, "%ld", &pid) == 1) {
        if (pid > 1 && kill((pid_t)pid, value) != 0 && errno != ESRCH) {
            fclose(input); return -1;
        }
    }
    int failed = ferror(input);
    fclose(input);
    return failed ? -1 : 0;
}

int main(int argc, char **argv) {
    if (argc < 4 || argv[2][0] != '-' || argv[2][1] != '-' || argv[2][2] != '\0') return 126;
    if (prctl(PR_SET_CHILD_SUBREAPER, 1L, 0L, 0L, 0L) != 0) return 126;
    struct sigaction handler = {0};
    handler.sa_handler = receive_signal;
    sigemptyset(&handler.sa_mask);
    if (sigaction(SIGTERM, &handler, NULL) != 0 || sigaction(SIGINT, &handler, NULL) != 0) return 126;
    pid_t primary = fork();
    if (primary < 0) return 126;
    if (primary == 0) {
        signal(SIGTERM, SIG_DFL); signal(SIGINT, SIG_DFL);
        /* Isolate the command group from the supervisor. Descendants may make
         * their own groups; subreaper ownership survives that operation. */
        if (setpgid(0, 0) != 0) _exit(126);
        execvp(argv[3], &argv[3]);
        _exit(errno == ENOENT ? 127 : 126);
    }
    int primary_done = 0, primary_status = 0, safe = 0, failed = 0, reaped = 0;
    long long cleanup_started = 0;
    for (;;) {
        int status;
        pid_t pid;
        while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
            reaped++;
            if (pid == primary) { primary_done = 1; primary_status = status; }
        }
        if (pid < 0 && errno != ECHILD && errno != EINTR) { failed = 1; break; }
        if (primary_done && pid < 0 && errno == ECHILD) { safe = 1; break; }
        if (primary_done || interrupted) {
            if (!cleanup_started) cleanup_started = milliseconds();
            long long elapsed = milliseconds() - cleanup_started;
            /* Killing a direct child reparents its remaining children here.
             * Repeat until waitpid proves ECHILD, including double-forks. */
            if (signal_children(elapsed < 200 ? SIGTERM : SIGKILL) != 0) { failed = 1; break; }
            if (elapsed > 5000) { failed = 1; break; }
        }
        struct timespec pause = {0, 10000000};
        nanosleep(&pause, NULL);
    }
    int descriptor = open(argv[1], O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (descriptor < 0) return 126;
    FILE *report = fdopen(descriptor, "w");
    if (!report) { close(descriptor); return 126; }
    int exit_code = primary_done && WIFEXITED(primary_status) ? WEXITSTATUS(primary_status) : -1;
    int exit_signal = primary_done && WIFSIGNALED(primary_status) ? WTERMSIG(primary_status) : 0;
    fprintf(report, "{\"schema\":\"teaql.owned-subreaper/v1\",\"safe\":%s,"
            "\"primaryDone\":%s,\"primaryExit\":%d,\"primarySignal\":%d,\"reaped\":%d}\n",
            safe && !failed ? "true" : "false", primary_done ? "true" : "false", exit_code, exit_signal, reaped);
    if (fclose(report) != 0) return 126;
    if (!safe || failed) return 126;
    if (exit_signal) { signal(exit_signal, SIG_DFL); raise(exit_signal); return 128 + exit_signal; }
    return exit_code;
}
