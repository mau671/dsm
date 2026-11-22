#ifndef DSM_MEM_H
#define DSM_MEM_H

#include <stdint.h>
#include <stddef.h>
#include <pthread.h>
#include <semaphore.h>
#include "dsm_types.h"
#include "config.h"

/* Registro local de estado de cada página en la vista de memoria */
typedef struct {
    uint8_t  state;          /* DSM_PAGE_I, S, M, IS_PEND, IM_PEND, SM_PEND */
    uint8_t  pend_inval;     /* Invalidacion solicitada por el home, aun sin aplicar */
    uint8_t  pend_down;      /* Degradacion M->S solicitada, aun sin aplicar */
    uint8_t  reserved;
    uint32_t version;
    uint32_t tseq_seen;
    /** Proceso que reservó esta página con dsm_alloc(); 0 si está libre. */
    uint32_t pid;
    /** Índice de la página dentro del bloque reservado (0..npages-1). */
    uint32_t order;
} page_local_t;

/* Estado del marco de respaldo de una página. DSM_BACKING_VALID contiene la
 * versión que el nodo custodia como home; DSM_BACKING_NONE indica que la página
 * nunca se persistió y debe leerse como memoria no inicializada. */
#define DSM_BACKING_NONE  0u
#define DSM_BACKING_VALID 1u

typedef struct dsm_mem {
    int              memfd;
    uint8_t         *view;           /* Mapeo en BASE_ADDR (permisos por página) */
    uint8_t         *alias;          /* Segundo mapeo RW del mismo memfd para red */
    size_t           view_size;
    size_t           page_size;
    uint32_t         npages;         /* view_size / page_size */
    uint32_t         backing_pages;
    uint8_t         *backing;        /* Respaldo físico de páginas home en RAM */
    uint32_t        *frames;         /* Marco asignado a cada página: npages entradas */
    uint8_t         *backing_state;  /* DSM_BACKING_* por página: npages entradas */
    uint8_t         *frame_used;     /* Bitmap de marcos ocupados del respaldo */
    pthread_mutex_t  frames_mu;      /* Protege frames, backing_state y frame_used */
    page_local_t    *local;          /* npages entradas de estado */
    /*
     * El estado MSI de cada página se protege con un semáforo binario por
     * franja porque la exclusión protege un contador (tseq_seen). El
     * directorio, que guarda el owner y la lista de copias, se protege con un
     * mutex, porque ahí la exclusión es sobre índices y mapas de bits y no
     * sobre un contador.
     */
    sem_t           *stripe_sems;    /* Un semáforo por stripe, valor inicial 1 */
    uint32_t         nstripes;
} dsm_mem_t;

int      dsm_mem_init(dsm_mem_t *m, const dsm_config_t *cfg);
void     dsm_mem_destroy(dsm_mem_t *m);
int      dsm_mem_set_prot(dsm_mem_t *m, uint32_t page, int prot);
void     dsm_mem_install_page(dsm_mem_t *m, uint32_t page, const void *src);
uint8_t *dsm_mem_page_ptr(dsm_mem_t *m, uint32_t page);
page_local_t *dsm_mem_local(dsm_mem_t *m, uint32_t page);
uint32_t dsm_mem_frame_alloc(dsm_mem_t *m);
void     dsm_mem_frame_free(dsm_mem_t *m, uint32_t frame);
uint32_t dsm_mem_page_frame(dsm_mem_t *m, uint32_t page);
uint8_t *dsm_mem_frame_ptr(dsm_mem_t *m, uint32_t frame);
int      dsm_mem_backing_store(dsm_mem_t *m, uint32_t page, const void *src);
int      dsm_mem_backing_load(dsm_mem_t *m, uint32_t page, void *dst);
bool     dsm_mem_backing_has(dsm_mem_t *m, uint32_t page);
int      dsm_mem_backing_drop(dsm_mem_t *m, uint32_t page);
/* Exclusión del stripe de una página sobre el semáforo de la tabla de estados. */
void     dsm_mem_stripe_lock(dsm_mem_t *m, uint32_t page);
void     dsm_mem_stripe_unlock(dsm_mem_t *m, uint32_t page);

#endif /* DSM_MEM_H */
