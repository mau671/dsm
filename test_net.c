/* test_net.c - prueba aislada de la capa de red.
 *   Servidor: ./test_net server <puerto>
 *   Cliente:  ./test_net client <ip> <puerto> [num_peticiones]
 */
#include "protocol.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

/* Patrón determinista: el byte i de la página p vale (p + i) & 0xFF.
 * Así el cliente puede verificar que los 4096 bytes llegaron intactos. */
static void fill_page(unsigned char *buf, uint32_t page_num)
{
    for (size_t i = 0; i < PAGE_SIZE; i++)
        buf[i] = (unsigned char)((page_num + i) & 0xFF);
}

/* Reemplaza al handle_message "débil" de network.c */
void handle_message(const struct msg_header *header, const void *payload, int fd)
{
    (void)payload;
    if (header->type == MSG_PAGE_REQUEST) {
        unsigned char page[PAGE_SIZE];
        struct msg_header resp = {0};

        fill_page(page, header->page_num);
        resp.type = MSG_PAGE_DATA;
        resp.sender_id = 99;               /* id falso del servidor */
        resp.seq = header->seq;            /* se devuelve el mismo seq */
        resp.page_num = header->page_num;
        resp.payload_len = PAGE_SIZE;
        if (send_msg(fd, &resp, page) < 0)
            fprintf(stderr, "[server] fallo al responder pagina %u\n",
                    (unsigned)header->page_num);
        else
            printf("[server] PAGE_REQUEST pagina %u (seq %u) -> PAGE_DATA\n",
                   (unsigned)header->page_num, (unsigned)header->seq);
    } else {
        printf("[server] mensaje tipo %u ignorado\n", (unsigned)header->type);
    }
}

static int run_server(uint16_t port)
{
    pthread_t thread;
    int fd = start_listener_thread(port, 16, &thread);

    if (fd < 0)
        return 1;
    printf("[server] escuchando en el puerto %u (Ctrl+C para salir)\n", port);
    pthread_join(thread, NULL);
    return 0;
}

static int run_client(const char *ip, uint16_t port, int count)
{
    static unsigned char payload[MSG_MAX_PAYLOAD];
    int ok = 0, bad = 0;

    for (int n = 0; n < count; n++) {
        struct msg_header req = {0}, resp = {0};
        uint32_t page = (uint32_t)(n % 256);

        req.type = MSG_PAGE_REQUEST;
        req.sender_id = 1;
        req.seq = (uint32_t)n;
        req.page_num = page;
        req.payload_len = 0;

        if (send_request(ip, port, &req, NULL, &resp, payload) < 0) {
            bad++;
            continue;
        }
        int valid = resp.type == MSG_PAGE_DATA &&
                    resp.seq == req.seq &&
                    resp.page_num == page &&
                    resp.payload_len == PAGE_SIZE;
        for (size_t i = 0; valid && i < PAGE_SIZE; i++)
            if (payload[i] != (unsigned char)((page + i) & 0xFF))
                valid = 0;
        valid ? ok++ : bad++;
    }
    printf("[client] %d correctas, %d fallidas de %d\n", ok, bad, count);
    return bad == 0 ? 0 : 1;
}

int main(int argc, char **argv)
{
    if (argc >= 3 && strcmp(argv[1], "server") == 0)
        return run_server((uint16_t)atoi(argv[2]));
    if (argc >= 4 && strcmp(argv[1], "client") == 0)
        return run_client(argv[2], (uint16_t)atoi(argv[3]),
                          argc >= 5 ? atoi(argv[4]) : 1);
    fprintf(stderr, "Uso:\n  %s server <puerto>\n  %s client <ip> <puerto> [n]\n",
            argv[0], argv[0]);
    return 1;
}