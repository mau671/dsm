#include "protocol.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#ifndef NETWORK_CONNECT_TIMEOUT_SEC
#define NETWORK_CONNECT_TIMEOUT_SEC 5
#endif

#ifndef NETWORK_IO_TIMEOUT_SEC
#define NETWORK_IO_TIMEOUT_SEC 5
#endif

/*
 * El proyecto puede proporcionar su propio callback. La versión débil permite
 * enlazar esta capa de red sin obligar a definirlo mientras se desarrolla.
 */
extern void handle_message(const struct msg_header *header,
                           const void *payload, int fd)
    __attribute__((weak));

ssize_t send_all(int sockfd, const void *buf, size_t len)
{
    /*
     * Envía un bloque completo de datos, repitiendo send() cuando la llamada
     * solo transmite una parte del bloque o es interrumpida por una señal.
     * Devuelve la cantidad total enviada o -1 si la conexión falla.
     */
    const unsigned char *data = buf;
    size_t sent = 0;

    while (sent < len) { 
        ssize_t result = send(sockfd, data + sent, len - sent, MSG_NOSIGNAL);

        if (result > 0) {
            sent += (size_t)result;
        } else if (result < 0 && errno == EINTR) {
            continue;
        } else if (result == 0) {
            errno = EPIPE;
            perror("send");
            return -1;
        } else {
            perror("send");
            return -1;
        }
    }

    return (ssize_t)sent;
}

ssize_t recv_all(int sockfd, void *buf, size_t len)
{
    /*
     * Recibe exactamente len bytes, repitiendo recv() hasta completar el
     * bloque. Devuelve la cantidad recibida o -1 si la conexión se cierra o
     * se produce un error de recepción.
     */
    unsigned char *data = buf;
    size_t received = 0;

    while (received < len) {
        ssize_t result = recv(sockfd, data + received, len - received, 0);

        if (result > 0) {
            received += (size_t)result;
        } else if (result < 0 && errno == EINTR) {
            continue;
        } else if (result == 0) {
            errno = ECONNRESET;
            perror("recv");
            return -1;
        } else {
            perror("recv");
            return -1;
        }
    }

    return (ssize_t)received;
}

int send_msg(int sockfd, const struct msg_header *header, const void *payload)
{
    /*
     * Valida un mensaje, convierte su cabecera al orden de bytes de red y
     * envía primero la cabecera y después el payload opcional. Devuelve 0 si
     * el mensaje se envía correctamente o -1 ante una entrada inválida o un
     * error de comunicación.
     */
    struct msg_header wire_header;

    if (header == NULL || (header->payload_len != 0 && payload == NULL)) {
        errno = EINVAL;
        perror("send_msg");
        return -1;
    }
    if (header->payload_len > MSG_MAX_PAYLOAD) {
        errno = EMSGSIZE;
        perror("send_msg");
        return -1;
    }

    /* Convert every field explicitly; the packed layout is not relied on. */
    wire_header.type = htons(header->type);
    wire_header.sender_id = htons(header->sender_id);
    wire_header.seq = htonl(header->seq);
    wire_header.page_num = htonl(header->page_num);
    wire_header.access = htons(header->access);
    wire_header.payload_len = htonl(header->payload_len);

    if (send_all(sockfd, &wire_header, sizeof(wire_header)) < 0)
        return -1;
    if (header->payload_len != 0 &&
        send_all(sockfd, payload, header->payload_len) < 0)
        return -1;

    return 0;
}

int recv_msg(int sockfd, struct msg_header *header, void *payload)
{
    /*
     * Recibe una cabecera y su payload opcional, convierte los campos desde
     * el orden de bytes de red y los guarda en los buffers proporcionados.
     * Rechaza payloads mayores que MSG_MAX_PAYLOAD y devuelve 0 en éxito o
     * -1 si los datos son inválidos o la recepción falla.
     */
    struct msg_header wire_header;

    if (header == NULL) {
        errno = EINVAL;
        perror("recv_msg");
        return -1;
    }
    if (recv_all(sockfd, &wire_header, sizeof(wire_header)) < 0)
        return -1;

    header->type = ntohs(wire_header.type);
    header->sender_id = ntohs(wire_header.sender_id);
    header->seq = ntohl(wire_header.seq);
    header->page_num = ntohl(wire_header.page_num);
    header->access = ntohs(wire_header.access);
    header->payload_len = ntohl(wire_header.payload_len);

    if (header->payload_len > MSG_MAX_PAYLOAD) {
        errno = EMSGSIZE;
        perror("recv_msg");
        return -1;
    }
    if (header->payload_len != 0 && payload == NULL) {
        errno = EINVAL;
        perror("recv_msg");
        return -1;
    }
    if (header->payload_len != 0 &&
        recv_all(sockfd, payload, header->payload_len) < 0)
        return -1;

    return 0;
}

int connect_to_node(const char *ip, uint16_t port)
{
    /*
     * Crea una conexión TCP al nodo indicado. El establecimiento se realiza
     * de forma no bloqueante para aplicar NETWORK_CONNECT_TIMEOUT_SEC; una
     * vez conectado, configura SO_RCVTIMEO y SO_SNDTIMEO con
     * NETWORK_IO_TIMEOUT_SEC. Devuelve el descriptor o -1 en caso de error.
     */
    int fd;
    int flags;
    int result;
    int socket_error;
    socklen_t socket_error_len = sizeof(socket_error);
    struct pollfd poll_descriptor;
    struct sockaddr_in address;
    struct timeval timeout;

    if (ip == NULL || NETWORK_CONNECT_TIMEOUT_SEC < 0 ||
        NETWORK_IO_TIMEOUT_SEC < 0) {
        errno = EINVAL;
        perror("connect_to_node");
        return -1;
    }

    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &address.sin_addr) != 1) {
        errno = EINVAL;
        perror("inet_pton");
        return -1;
    }

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }

    flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("fcntl");
        close(fd);
        return -1;
    }

    result = connect(fd, (struct sockaddr *)&address, sizeof(address));
    if (result < 0 && errno != EINPROGRESS) {
        perror("connect");
        close(fd);
        return -1;
    }
    if (result < 0) {
        poll_descriptor.fd = fd;
        poll_descriptor.events = POLLOUT;
        poll_descriptor.revents = 0;
        do {
            result = poll(&poll_descriptor, 1,
                          NETWORK_CONNECT_TIMEOUT_SEC * 1000);
        } while (result < 0 && errno == EINTR);
        if (result <= 0) {
            if (result == 0)
                errno = ETIMEDOUT;
            perror("poll");
            close(fd);
            return -1;
        }

        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error,
                       &socket_error_len) < 0) {
            perror("getsockopt");
            close(fd);
            return -1;
        }
        if (socket_error != 0) {
            errno = socket_error;
            perror("connect");
            close(fd);
            return -1;
        }
    }

    if (fcntl(fd, F_SETFL, flags) < 0) {
        perror("fcntl");
        close(fd);
        return -1;
    }

    timeout.tv_sec = NETWORK_IO_TIMEOUT_SEC;
    timeout.tv_usec = 0;
    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                   sizeof(timeout)) < 0 ||
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout,
                   sizeof(timeout)) < 0) {
        perror("setsockopt");
        close(fd);
        return -1;
    }

    return fd;
}

int send_request(const char *ip, uint16_t port,
                 const struct msg_header *req, const void *req_payload,
                 struct msg_header *resp, void *resp_payload)
{
    /*
     * Conecta con un nodo, envía la solicitud y espera su respuesta usando
     * el protocolo de mensajes. Cierra el socket tanto en éxito como en
     * error, y devuelve 0 si ambas operaciones se completan correctamente.
     */
    int fd;
    int result;

    fd = connect_to_node(ip, port);
    if (fd < 0)
        return -1;

    result = send_msg(fd, req, req_payload);
    if (result == 0)
        result = recv_msg(fd, resp, resp_payload);

    close(fd);
    return result;
}

int start_listener(uint16_t port, int backlog)
{
    /*
     * Crea un socket TCP IPv4, permite reutilizar su dirección, lo enlaza a
     * todas las interfaces locales en el puerto indicado y lo deja escuchando.
     * Devuelve el descriptor del socket o -1 si alguna operación falla.
     */
    int fd;
    int reuse = 1;
    struct sockaddr_in address;

    if (backlog < 0) {
        errno = EINVAL;
        perror("listen");
        return -1;
    }

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        perror("setsockopt");
        close(fd);
        return -1;
    }

    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(port);

    if (bind(fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind");
        close(fd);
        return -1;
    }
    if (listen(fd, backlog) < 0) {
        perror("listen");
        close(fd);
        return -1;
    }

    return fd;
}

static void *connection_thread(void *argument)
{
    /*
     * Atiende una conexión aceptada: recibe un mensaje, lo entrega al
     * callback handle_message cuando está disponible y cierra el descriptor
     * al terminar. También libera la memoria usada para pasar el descriptor.
     */
    int fd = *(int *)argument;
    unsigned char payload[MSG_MAX_PAYLOAD];
    struct msg_header header;

    free(argument);
    if (recv_msg(fd, &header, payload) == 0) {
        if (handle_message != NULL)
            handle_message(&header, payload, fd);
        else {
            errno = ENOSYS;
            perror("handle_message");
        }
    }
    close(fd);
    return NULL;
}

void *network_listener_thread(void *argument)
{
    /*
     * Ejecuta continuamente accept() sobre el descriptor recibido y crea un
     * hilo separado para cada conexión. El argumento se libera al comenzar;
     * el hilo termina cuando accept() falla por una razón distinta de EINTR.
     */
    int listener_fd = *(int *)argument;

    free(argument);
    for (;;) {
        int *connection_fd = malloc(sizeof(*connection_fd));
        pthread_t thread;
        int result;

        if (connection_fd == NULL) {
            perror("malloc");
            return NULL;
        }

        *connection_fd = accept(listener_fd, NULL, NULL);
        if (*connection_fd < 0) {
            if (errno == EINTR) {
                free(connection_fd);
                continue;
            }
            perror("accept");
            free(connection_fd);
            return NULL;
        }

        result = pthread_create(&thread, NULL, connection_thread, connection_fd);
        if (result != 0) {
            errno = result;
            perror("pthread_create");
            close(*connection_fd);
            free(connection_fd);
            continue;
        }
        result = pthread_detach(thread);
        if (result != 0) {
            errno = result;
            perror("pthread_detach");
            /*
             * El hilo ya posee el descriptor; no se cierra aquí para evitar
             * una carrera con connection_thread.
             */
        }
    }
}

int start_listener_thread(uint16_t port, int backlog, pthread_t *thread)
{
    /*
     * Crea un socket de escucha y arranca network_listener_thread con ese
     * descriptor. Guarda el identificador del hilo en thread y devuelve el
     * descriptor del listener, que debe cerrarse cuando ya no sea necesario.
     */
    int listener_fd;
    int *thread_argument;
    int result;

    if (thread == NULL) {
        errno = EINVAL;
        perror("start_listener_thread");
        return -1;
    }

    listener_fd = start_listener(port, backlog);
    if (listener_fd < 0)
        return -1;

    thread_argument = malloc(sizeof(*thread_argument));
    if (thread_argument == NULL) {
        perror("malloc");
        close(listener_fd);
        return -1;
    }
    *thread_argument = listener_fd;

    result = pthread_create(thread, NULL, network_listener_thread,
                            thread_argument);
    if (result != 0) {
        errno = result;
        perror("pthread_create");
        close(listener_fd);
        free(thread_argument);
        return -1;
    }

    return listener_fd;
}
