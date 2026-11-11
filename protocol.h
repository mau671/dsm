#ifndef DSM_PROTOCOL_H
#define DSM_PROTOCOL_H

#include <stddef.h>
#include <pthread.h>
#include <stdint.h>
#include <sys/types.h>

/* Tamaño de una página de memoria. Puede redefinirse al compilar. */
#ifndef PAGE_SIZE
#define PAGE_SIZE 4096U
#endif

/* Tamaño máximo permitido para el contenido de un mensaje. */
#define MSG_MAX_PAYLOAD (PAGE_SIZE)

/* Tipos de mensajes intercambiados entre los nodos DSM. */
enum msg_type {
    MSG_JOIN = 1,
    MSG_HEARTBEAT,
    MSG_PAGE_REQUEST,
    MSG_PAGE_DATA,
    MSG_INVALIDATE,
    MSG_INVALIDATE_ACK,
    MSG_ERROR
};

/* Permisos solicitados o concedidos para una página. */
enum access_type {
    READ = 1,
    WRITE
};

/*
 * Cabecera común de todos los mensajes.
 * Los campos se serializan en orden de red con htonl/htons antes de enviar
 * y se convierten de nuevo con ntohl/ntohs después de recibirlos.
 */
struct msg_header {
    uint16_t type;       /* Tipo de mensaje; contiene un valor de enum msg_type. */
    uint16_t sender_id;  /* Identificador del nodo que envió el mensaje. */
    uint32_t seq;        /* Número de secuencia para asociar mensajes y respuestas. */
    uint32_t page_num;   /* Número de la página DSM a la que se refiere el mensaje. */
    uint16_t access;     /* Permiso solicitado o concedido: READ o WRITE. */
    uint32_t payload_len; /* Cantidad de bytes que siguen a esta cabecera. */
} __attribute__((packed));

/*
 * Envía exactamente len bytes, aunque send() tenga que repetirse.
 * Devuelve el número de bytes enviados o un valor negativo si ocurre un error.
 */
ssize_t send_all(int sockfd, const void *buf, size_t len);

/*
 * Recibe exactamente len bytes, aunque recv() tenga que repetirse.
 * Devuelve el número de bytes recibidos o un valor negativo si ocurre un error.
 */
ssize_t recv_all(int sockfd, void *buf, size_t len);

/*
 * Envía la cabecera y el payload de un mensaje.
 * La implementación debe convertir los campos de la cabecera a orden de red.
 */
int send_msg(int sockfd, const struct msg_header *header,
             const void *payload);

/*
 * Recibe una cabecera y su payload.
 * La implementación debe convertir los campos de la cabecera desde orden de red.
 * El buffer de payload debe tener capacidad para MSG_MAX_PAYLOAD bytes.
 */
int recv_msg(int sockfd, struct msg_header *header, void *payload);

/*
 * Conecta con un nodo TCP y configura los tiempos de espera de la conexión
 * y de las operaciones de lectura y escritura.
 */
int connect_to_node(const char *ip, uint16_t port);

/*
 * Conecta con un nodo, envía una solicitud, recibe su respuesta y cierra
 * la conexión. Devuelve 0 si todo se completa correctamente o -1 si falla.
 */
int send_request(const char *ip, uint16_t port,
                const struct msg_header *req, const void *req_payload,
                struct msg_header *resp, void *resp_payload);

/*
 * Crea, enlaza y deja escuchando un socket TCP en el puerto indicado.
 * Devuelve el descriptor del socket o un valor negativo si ocurre un error.
 */
int start_listener(uint16_t port, int backlog);

/*
 * Ejecuta el bucle accept en un hilo y crea un hilo separado por conexión.
 * El descriptor devuelto por start_listener_thread debe cerrarse cuando ya no
 * se necesite el listener; cerrar ese descriptor hace que accept falle y el
 * hilo termine.
 */
void *network_listener_thread(void *argument);
int start_listener_thread(uint16_t port, int backlog, pthread_t *thread);

#endif /* DSM_PROTOCOL_H */
