#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define CONF_PATH "/tmp/dsm_two_nodes_integration.conf"
#define TEST_APP "./build/debug/test_basico"
#define LINE_TIMEOUT_MS 15000

typedef struct {
    pid_t pid;
    FILE *input;
    FILE *output;
} child_t;

static void write_config(void)
{
    FILE *file = fopen(CONF_PATH, "w");
    if (!file) return;

    fprintf(file,
            "CLUSTER_ID=0x534F3031\n"
            "PAGE_SIZE=4096\n"
            "BASE_ADDR=0x200000000000\n"
            "VIEW_SIZE=4294967296\n"
            "MAX_RUNTIME_NODES=256\n"
            "VNODES_PER_MB=16\n"
            "CHUNK_PAGES=256\n"
            "REDUNDANCY=NONE\n"
            "RAID_K=2\n"
            "NODE_MEM=67108864\n"
            "PORT_MIN=27660\n"
            "PORT_MAX=27699\n"
            "PEER_HOSTS=127.0.0.1\n"
            "CONNECT_TIMEOUT_MS=300\n"
            "DISCOVERY_TIMEOUT_MS=300\n"
            "SEND_TIMEOUT_MS=2000\n"
            "REQUEST_TIMEOUT_MS=3000\n"
            "REQUEST_RETRIES=3\n"
            "RECONNECT_MIN_MS=100\n"
            "RECONNECT_MAX_MS=1500\n"
            "HB_INTERVAL_MS=200\n"
            "HB_SUSPECT=3\n"
            "HB_DEAD=6\n"
            "WORKER_THREADS=2\n"
            "DISPATCHER_THREADS=1\n"
            "PAGE_LOCK_STRIPES=64\n"
            "HOME_SLACK=1.25\n"
            "WAIT_POOL_ENTRIES=1024\n"
            "LOCK_TIMEOUT_MS=5000\n"
            "WORKER_QUEUE_CAP=1024\n"
            "SYNC_THREADS=2\n"
            "OUTPUT_DIR=/tmp\n"
            "DSM_LOG_LEVEL=ERROR\n");
    fclose(file);
}

static child_t start_child(const char *role)
{
    int input_pipe[2];
    int output_pipe[2];
    child_t child = { .pid = -1, .input = NULL, .output = NULL };
    if (pipe(input_pipe) != 0 || pipe(output_pipe) != 0) return child;

    pid_t pid = fork();
    if (pid < 0) return child;
    if (pid == 0) {
        int null_fd = open("/dev/null", O_WRONLY);
        dup2(input_pipe[0], STDIN_FILENO);
        dup2(output_pipe[1], STDOUT_FILENO);
        if (null_fd >= 0) dup2(null_fd, STDERR_FILENO);
        close(input_pipe[0]);
        close(input_pipe[1]);
        close(output_pipe[0]);
        close(output_pipe[1]);
        if (null_fd >= 0) close(null_fd);
        execl(TEST_APP, TEST_APP, role, CONF_PATH, (char *)NULL);
        _exit(127);
    }

    close(input_pipe[0]);
    close(output_pipe[1]);
    child.pid = pid;
    child.input = fdopen(input_pipe[1], "w");
    child.output = fdopen(output_pipe[0], "r");
    if (child.input) setvbuf(child.input, NULL, _IOLBF, 0);
    if (child.output) setvbuf(child.output, NULL, _IONBF, 0);
    return child;
}

static bool read_line(child_t *child, char *line, size_t capacity)
{
    struct pollfd pfd = { .fd = fileno(child->output), .events = POLLIN };
    int rc;
    do {
        rc = poll(&pfd, 1, LINE_TIMEOUT_MS);
    } while (rc < 0 && errno == EINTR);
    if (rc <= 0) return false;
    return fgets(line, (int)capacity, child->output) != NULL;
}

static bool expect_line(child_t *child, const char *expected)
{
    char line[512];
    if (!read_line(child, line, sizeof(line))) {
        printf("  FAIL: no se recibio '%s' dentro del timeout\n", expected);
        return false;
    }
    printf("  %s", line);
    return strstr(line, expected) != NULL;
}

static void send_command(child_t *child, const char *command)
{
    if (child->input) {
        fprintf(child->input, "%s\n", command);
        fflush(child->input);
    }
}

static void stop_child(child_t *child)
{
    if (child->input) fclose(child->input);
    if (child->output) fclose(child->output);
    if (child->pid > 0) {
        int status = 0;
        if (waitpid(child->pid, &status, WNOHANG) == 0) {
            kill(child->pid, SIGTERM);
            waitpid(child->pid, &status, 0);
        }
    }
}

int main(void)
{
    child_t node_a = { 0 };
    child_t node_b = { 0 };
    bool pass = true;

    setvbuf(stdout, NULL, _IOLBF, 0);
    write_config();
    printf("=== Integracion DSM: dos nodos y flujo MSI ===\n");

    node_a = start_child("A");
    if (node_a.pid < 0 || !node_a.input || !node_a.output) {
        printf("FAIL: no se pudo iniciar nodo A\n");
        stop_child(&node_a);
        unlink(CONF_PATH);
        return 1;
    }
    pass = expect_line(&node_a, "A_AFTER_WRITE stats") &&
           expect_line(&node_a, "A_READY PASS");

    node_b = start_child("B");
    if (node_b.pid < 0 || !node_b.input || !node_b.output) {
        printf("FAIL: no se pudo iniciar nodo B\n");
        pass = false;
    } else {
        pass = expect_line(&node_b, "B_BEFORE_READ stats") && pass;
        pass = expect_line(&node_b, "B_READ PASS") && pass;
        pass = expect_line(&node_b, "B_AFTER_READ stats") && pass;
        pass = expect_line(&node_b, "B_AFTER_WRITE PASS") && pass;
        pass = expect_line(&node_b, "B_AFTER_WRITE stats") && pass;
        send_command(&node_a, "READ");
        pass = expect_line(&node_a, "A_AFTER_B_WRITE PASS") && pass;
        pass = expect_line(&node_a, "A_AFTER_READ stats") && pass;
    }

    send_command(&node_a, "QUIT");
    send_command(&node_b, "QUIT");
    stop_child(&node_a);
    stop_child(&node_b);
    unlink(CONF_PATH);

    printf("%s\n", pass ? "PASS: flujo de página entre dos nodos" :
                         "FAIL: flujo de página entre dos nodos");
    return pass ? 0 : 1;
}
