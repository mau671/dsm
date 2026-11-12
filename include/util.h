#ifndef DSM_UTIL_H
#define DSM_UTIL_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <netinet/in.h>

/* Tiempo monotónico en milisegundos */
uint64_t mono_ms(void);

/* Dormir durante una cantidad de milisegundos */
void sleep_ms(uint32_t ms);

/* Escritura garantizada de bloques completos sobre ARCHIVOS */
int dsm_write_full(int fd, const void *buf, size_t len);

/* Escritura garantizada sobre SOCKETS con MSG_NOSIGNAL: un peer que cierra a
 * mitad de la transferencia produce -1/EPIPE en vez de matar el proceso con
 * SIGPIPE. Reintenta EINTR. */
int dsm_send_full(int fd, const void *buf, size_t len);

/* Lectura garantizada de bloques completos sobre descriptores.
 * dsm_read_full_deadline acota el bloqueo total con poll()+deadline y
 * reintenta EINTR; es la unica ruta de lectura de red del proyecto (no se
 * configura SO_RCVTIMEO porque duplicaria el mecanismo). */
int dsm_read_full(int fd, void *buf, size_t len);
int dsm_read_full_deadline(int fd, void *buf, size_t len, uint32_t timeout_ms);

/* Conexion TCP con plazo: socket no bloqueante + connect + poll + SO_ERROR y
 * restauracion de las banderas originales. Devuelve el descriptor conectado o
 * DSM_ERR_TIMEOUT / DSM_ERR_NET. No instala timeouts de socket: las lecturas
 * usan dsm_read_full_deadline y las escrituras dsm_send_full. */
int dsm_connect_timeout(const struct sockaddr_in *addr, uint32_t timeout_ms);

/* Modificación de banderas sobre descriptores */
int set_cloexec(int fd);
int set_nonblock(int fd, bool nonblock);

/* Gestión de UUID persistente en ~/.dsm/<cluster_id>-<hostname>-<tag>.uuid */
int uuid_load_or_create_file(uint32_t cluster_id, const char *tag, uint8_t out[16]);
void uuid_to_str(const uint8_t uuid[16], char out[37]);

#endif /* DSM_UTIL_H */
