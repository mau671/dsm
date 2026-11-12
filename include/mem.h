#ifndef DSM_MEM_H
#define DSM_MEM_H

#include <stdint.h>
#include <stddef.h>
#include <pthread.h>
#include "dsm_types.h"
#include "config.h"

/* Registro local de estado de cada página en la vista de memoria */
typedef struct {
    uint8_t  state;          /* DSM_PAGE_I, S, M, IS_PEND, IM_PEND, SM_PEND */
    uint8_t  pend_inval;     /* Bandera de invalidación pendiente */
    uint8_t  pend_down;      /* Bandera de degradación pendiente */
    uint8_t  reserved;
    uint32_t version;
    uint32_t tseq_seen;
    uint32_t wait_head;      /* Cabeza de hilos locales esperando */
} page_local_t;

typedef struct dsm_mem {
    int              memfd;
    uint8_t         *view;           /* Mapeo en BASE_ADDR (permisos por página) */
    uint8_t         *alias;          /* Segundo mapeo RW del mismo memfd para red */
    size_t           view_size;
    size_t           page_size;
    uint32_t         npages;         /* view_size / page_size */
    uint32_t         backing_pages;
    uint8_t         *backing;        /* Respaldo físico de páginas home en RAM */
    page_local_t    *local;          /* npages entradas de estado */
    pthread_mutex_t *stripes;        /* Cerrejos particionados por página */
    uint32_t         nstripes;
} dsm_mem_t;

int      dsm_mem_init(dsm_mem_t *m, const dsm_config_t *cfg);
void     dsm_mem_destroy(dsm_mem_t *m);
int      dsm_mem_set_prot(dsm_mem_t *m, uint32_t page, int prot);
void     dsm_mem_install_page(dsm_mem_t *m, uint32_t page, const void *src);
void     dsm_mem_install_local(dsm_mem_t *m, uint32_t page);
uint8_t *dsm_mem_page_ptr(dsm_mem_t *m, uint32_t page);
page_local_t *dsm_mem_local(dsm_mem_t *m, uint32_t page);
uint32_t dsm_mem_frame_alloc(dsm_mem_t *m);
void     dsm_mem_frame_free(dsm_mem_t *m, uint32_t frame);
pthread_mutex_t *dsm_mem_stripe(dsm_mem_t *m, uint32_t page);

#endif /* DSM_MEM_H */
