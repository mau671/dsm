#include "mem.h"
#include "log.h"
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/mman.h>

int dsm_mem_init(dsm_mem_t *m, const dsm_config_t *cfg)
{
    memset(m, 0, sizeof(*m));
    m->page_size = cfg->page_size;
    m->view_size = cfg->view_size;
    m->npages    = (uint32_t)(m->view_size / m->page_size);
    m->nstripes  = cfg->page_lock_stripes;

    m->stripe_sems = calloc(m->nstripes, sizeof(sem_t));
    if (!m->stripe_sems) return DSM_ERR_MEM;

    for (uint32_t i = 0; i < m->nstripes; i++) {
        /* Cada stripe arranca libre: el semáforo actúa como cerrojo binario. */
        if (sem_init(&m->stripe_sems[i], 0, 1) != 0) {
            for (uint32_t j = 0; j < i; j++) {
                sem_destroy(&m->stripe_sems[j]);
            }
            free(m->stripe_sems);
            return DSM_ERR_MEM;
        }
    }

    m->local = calloc(m->npages, sizeof(page_local_t));
    if (!m->local) {
        for (uint32_t i = 0; i < m->nstripes; i++) {
            sem_destroy(&m->stripe_sems[i]);
        }
        free(m->stripe_sems);
        return DSM_ERR_MEM;
    }

    /* Respaldo físico de páginas home. Un cliente no custodia páginas, así que
     * su presupuesto de marcos es cero: la vista y el alias se crean igual
     * porque necesita acceder a la memoria de los nodos. */
    uint32_t backing_pages = 0;
    if (!cfg->client_mode) {
        backing_pages = (uint32_t)((cfg->node_mem * cfg->home_slack_permille / 1000) / m->page_size);
    }
    m->backing_pages = backing_pages;
    m->backing = calloc(m->backing_pages > 0 ? m->backing_pages : 1u, m->page_size);
    /* frames se indexa por página de la vista, no por marco: una página con
     * home local puede tener cualquier identificador hasta npages-1. */
    m->frames = malloc(m->npages * sizeof(uint32_t));
    m->backing_state = calloc(m->npages, sizeof(uint8_t));
    m->frame_used = calloc(m->backing_pages > 0 ? m->backing_pages : 1u,
                           sizeof(uint8_t));
    if (!m->backing || !m->frames || !m->backing_state || !m->frame_used) {
        free(m->backing);
        free(m->frames);
        free(m->backing_state);
        free(m->frame_used);
        free(m->local);
        for (uint32_t i = 0; i < m->nstripes; i++) {
            sem_destroy(&m->stripe_sems[i]);
        }
        free(m->stripe_sems);
        return DSM_ERR_MEM;
    }

    for (uint32_t i = 0; i < m->npages; i++) {
        m->frames[i] = DSM_PAGE_ID_NONE;
        m->backing_state[i] = DSM_BACKING_NONE;
    }
    if (pthread_mutex_init(&m->frames_mu, NULL) != 0) {
        /* El mutex no llegó a inicializarse: se liberan las estructuras ya
         * creadas sin destruirlo, igual que el resto de rutas de error. */
        free(m->backing);
        free(m->frames);
        free(m->backing_state);
        free(m->frame_used);
        free(m->local);
        for (uint32_t i = 0; i < m->nstripes; i++) {
            sem_destroy(&m->stripe_sems[i]);
        }
        free(m->stripe_sems);
        return DSM_ERR_MEM;
    }

    /* Doble mapeo con memfd_create */
    m->memfd = memfd_create("dsm-shared", MFD_CLOEXEC | MFD_ALLOW_SEALING);
    if (m->memfd < 0) {
        dsm_mem_destroy(m);
        return DSM_ERR_MEM;
    }

    if (ftruncate(m->memfd, (off_t)m->view_size) < 0) {
        dsm_mem_destroy(m);
        return DSM_ERR_MEM;
    }

    fcntl(m->memfd, F_ADD_SEALS, F_SEAL_SHRINK | F_SEAL_GROW);

    /* Vista global para la aplicación en BASE_ADDR */
    void *target_addr = (void *)cfg->base_addr;
#if defined(DSM_TSAN)
    if (cfg->base_addr_tsan != 0) {
        target_addr = (void *)cfg->base_addr_tsan;
    }
#endif

    m->view = mmap(target_addr, m->view_size, PROT_NONE,
                   MAP_SHARED | MAP_FIXED_NOREPLACE, m->memfd, 0);
    if (m->view == MAP_FAILED) {
        DSM_LOG_ERR("mmap de vista en %p fallo (MAP_FIXED_NOREPLACE)", target_addr);
        m->view = NULL;
        dsm_mem_destroy(m);
        return DSM_ERR_MEM;
    }

    /* Alias de red con permisos RW constantes */
    m->alias = mmap(NULL, m->view_size, PROT_READ | PROT_WRITE,
                    MAP_SHARED, m->memfd, 0);
    if (m->alias == MAP_FAILED) {
        DSM_LOG_ERR("mmap de alias RW fallo");
        m->alias = NULL;
        dsm_mem_destroy(m);
        return DSM_ERR_MEM;
    }

    DSM_LOG_INFO("Memoria DSM mapeada: Vista en %p, Alias en %p (%lu MB)",
                 (void *)m->view, (void *)m->alias, (unsigned long)(m->view_size >> 20));
    return DSM_OK;
}

void dsm_mem_destroy(dsm_mem_t *m)
{
    bool frames_ready = (m->frame_used != NULL);
    if (m->alias) munmap(m->alias, m->view_size);
    if (m->view) munmap(m->view, m->view_size);
    if (m->memfd >= 0) close(m->memfd);
    if (m->backing) free(m->backing);
    if (m->frames) free(m->frames);
    if (m->backing_state) free(m->backing_state);
    if (m->frame_used) free(m->frame_used);
    if (m->local) free(m->local);
    if (m->stripe_sems) {
        for (uint32_t i = 0; i < m->nstripes; i++) {
            sem_destroy(&m->stripe_sems[i]);
        }
        free(m->stripe_sems);
    }
    if (frames_ready) pthread_mutex_destroy(&m->frames_mu);
    memset(m, 0, sizeof(*m));
    m->memfd = -1;
}

int dsm_mem_set_prot(dsm_mem_t *m, uint32_t page, int prot)
{
    if (page >= m->npages) return DSM_ERR_INVALID;
    void *addr = m->view + ((uint64_t)page * m->page_size);
    if (mprotect(addr, m->page_size, prot) != 0) {
        return DSM_ERR_MEM;
    }
    return DSM_OK;
}

void dsm_mem_install_page(dsm_mem_t *m, uint32_t page, const void *src)
{
    if (page < m->npages && src) {
        memcpy(m->alias + ((uint64_t)page * m->page_size), src, m->page_size);
    }
}

uint8_t *dsm_mem_page_ptr(dsm_mem_t *m, uint32_t page)
{
    if (page >= m->npages) return NULL;
    return m->alias + ((uint64_t)page * m->page_size);
}

page_local_t *dsm_mem_local(dsm_mem_t *m, uint32_t page)
{
    if (page >= m->npages) return NULL;
    return &m->local[page];
}

uint32_t dsm_mem_frame_alloc(dsm_mem_t *m)
{
    if (!m->frame_used) return DSM_PAGE_ID_NONE;

    uint32_t frame = DSM_PAGE_ID_NONE;
    pthread_mutex_lock(&m->frames_mu);
    for (uint32_t i = 0; i < m->backing_pages; i++) {
        if (!m->frame_used[i]) {
            m->frame_used[i] = 1;
            frame = i;
            break;
        }
    }
    pthread_mutex_unlock(&m->frames_mu);
    return frame;
}

void dsm_mem_frame_free(dsm_mem_t *m, uint32_t frame)
{
    if (!m->frame_used || frame >= m->backing_pages) return;

    pthread_mutex_lock(&m->frames_mu);
    m->frame_used[frame] = 0;
    pthread_mutex_unlock(&m->frames_mu);
}

uint32_t dsm_mem_page_frame(dsm_mem_t *m, uint32_t page)
{
    if (!m->frames || page >= m->npages) return DSM_PAGE_ID_NONE;

    pthread_mutex_lock(&m->frames_mu);
    uint32_t frame = m->frames[page];
    pthread_mutex_unlock(&m->frames_mu);
    return frame;
}

uint8_t *dsm_mem_frame_ptr(dsm_mem_t *m, uint32_t frame)
{
    if (!m->backing || frame >= m->backing_pages) return NULL;
    return m->backing + ((uint64_t)frame * m->page_size);
}

/* Reserva el marco fijo de una página home la primera vez que se persiste.
 * Si el respaldo está lleno devuelve DSM_ERR_NO_SPACE. */
static int frame_ensure(dsm_mem_t *m, uint32_t page, uint32_t *out_frame)
{
    pthread_mutex_lock(&m->frames_mu);
    uint32_t frame = m->frames[page];
    if (frame == DSM_PAGE_ID_NONE) {
        for (uint32_t i = 0; i < m->backing_pages; i++) {
            if (!m->frame_used[i]) {
                m->frame_used[i] = 1;
                m->frames[page] = i;
                frame = i;
                break;
            }
        }
    }
    pthread_mutex_unlock(&m->frames_mu);

    if (frame == DSM_PAGE_ID_NONE) return DSM_ERR_NO_SPACE;
    *out_frame = frame;
    return DSM_OK;
}

int dsm_mem_backing_store(dsm_mem_t *m, uint32_t page, const void *src)
{
    if (!src || page >= m->npages) return DSM_ERR_INVALID;

    uint32_t frame = DSM_PAGE_ID_NONE;
    int rc = frame_ensure(m, page, &frame);
    if (rc != DSM_OK) return rc;

    memcpy(dsm_mem_frame_ptr(m, frame), src, m->page_size);

    pthread_mutex_lock(&m->frames_mu);
    m->backing_state[page] = DSM_BACKING_VALID;
    pthread_mutex_unlock(&m->frames_mu);
    return DSM_OK;
}

int dsm_mem_backing_load(dsm_mem_t *m, uint32_t page, void *dst)
{
    if (!dst || page >= m->npages) return DSM_ERR_INVALID;

    pthread_mutex_lock(&m->frames_mu);
    uint32_t frame = m->frames[page];
    uint8_t state = m->backing_state[page];
    if (frame != DSM_PAGE_ID_NONE && state == DSM_BACKING_VALID) {
        memcpy(dst, dsm_mem_frame_ptr(m, frame), m->page_size);
    }
    pthread_mutex_unlock(&m->frames_mu);

    return (frame == DSM_PAGE_ID_NONE || state != DSM_BACKING_VALID)
               ? DSM_ERR_NOT_FOUND : DSM_OK;
}

bool dsm_mem_backing_has(dsm_mem_t *m, uint32_t page)
{
    if (!m->backing_state || page >= m->npages) return false;

    pthread_mutex_lock(&m->frames_mu);
    bool valid = (m->backing_state[page] == DSM_BACKING_VALID);
    pthread_mutex_unlock(&m->frames_mu);
    return valid;
}

/* Libera el marco de una página que deja de ser home: el contenido deja de
 * estar respaldado por este nodo. */
int dsm_mem_backing_drop(dsm_mem_t *m, uint32_t page)
{
    if (!m->frames || page >= m->npages) return DSM_ERR_INVALID;

    pthread_mutex_lock(&m->frames_mu);
    uint32_t frame = m->frames[page];
    if (frame != DSM_PAGE_ID_NONE) {
        memset(dsm_mem_frame_ptr(m, frame), 0, m->page_size);
        m->frame_used[frame] = 0;
        m->frames[page] = DSM_PAGE_ID_NONE;
    }
    m->backing_state[page] = DSM_BACKING_NONE;
    pthread_mutex_unlock(&m->frames_mu);
    return DSM_OK;
}

/*
 * Exclusión del stripe. El semáforo del stripe, inicializado en uno, actúa
 * como cerrojo binario de la tabla de estados de página; se reintenta ante
 * EINTR para que una señal ajena no deje la sección a medio tomar.
 */
void dsm_mem_stripe_lock(dsm_mem_t *m, uint32_t page)
{
    uint32_t idx = page % m->nstripes;
    while (sem_wait(&m->stripe_sems[idx]) != 0 && errno == EINTR) {
        /* reintentar */
    }
}

void dsm_mem_stripe_unlock(dsm_mem_t *m, uint32_t page)
{
    sem_post(&m->stripe_sems[page % m->nstripes]);
}
