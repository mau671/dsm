#ifndef DSM_SIGSEGV_H
#define DSM_SIGSEGV_H

#include <stdint.h>
#include <stdbool.h>
#include "dsm_types.h"

#define DSM_ACCESS_READ  0
#define DSM_ACCESS_WRITE 1

/* Ranura de fallo por hilo de aplicación (16 B) */
typedef struct {
    volatile int done;       /* 0: en espera, 1: resuelto */
    uint32_t     page;
    uint32_t     next;
    uint8_t      access;     /* DSM_ACCESS_READ / WRITE */
    uint8_t      pad[3];
} fault_slot_t;

_Static_assert(sizeof(fault_slot_t) == 16, "fault_slot_t debe ser 16 B");

int           dsm_sig_install(void);
void          dsm_sig_restore(void);
dsm_status_t  dsm_sig_thread_attach(void);
dsm_status_t  dsm_sig_thread_detach(void);
int           faultq_init(void);
bool          faultq_push(fault_slot_t *s);
fault_slot_t *faultq_pop(void);

#endif /* DSM_SIGSEGV_H */
