
#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>

static int gears_http_signal_write = -1;

// - a handler may only make async-signal-safe calls, which Scala Native code is not: it writes to a pipe
static void gears_http_on_signal(int sig) {
    int saved = errno;
    unsigned char c = (unsigned char)sig;
    ssize_t ignored = write(gears_http_signal_write, &c, 1);
    (void)ignored;
    errno = saved;
}

int gears_http_signal_pipe(void) {
    static int read_end = -1;
    if (read_end >= 0) return read_end;
    int fds[2];
    if (pipe(fds) != 0) return -1;
    fcntl(fds[0], F_SETFD, FD_CLOEXEC);
    fcntl(fds[1], F_SETFD, FD_CLOEXEC);
    gears_http_signal_write = fds[1];
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = gears_http_on_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    if (sigaction(SIGTERM, &sa, NULL) != 0 || sigaction(SIGINT, &sa, NULL) != 0) return -1;
    read_end = fds[0];
    return read_end;
}
