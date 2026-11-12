#include "mem.h"
#include "log.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

int dsm_mem_init(dsm_mem_t *m, const dsm_config_t *cfg)
{
    memset(m, 0, sizeof(*m));
    m->page_size = cfg->page_size;
    m->view_size = cfg->view_size;
    m->npages    = (uint32_t)(m->view_size / m->page_size);
    m->nstripes  = cfg->page_lock_stripes;

    m->stripes = calloc(m->nstripes, sizeof(pthread_mutex_t));
    if (!m->stripes) return DSM_ERR_MEM;

    for (uint32_t i = 0; i < m->nstripes; i++) {
        pthread_mutex_init(&m->stripes[i], NULL);
    }

    m->local = calloc(m->npages, sizeof(page_local_t));
    if (!m->local) {
        free(m->stripes);
        return DSM_ERR_MEM;
    }

    /* Respaldo físico de páginas home */
    m->backing_pages = (uint32_t)((cfg->node_mem * cfg->home_slack_permille / 1000) / m->page_size);
    m->backing = calloc(m->backing_pages, m->page_size);
    if (!m->backing) {
        free(m->local);
        free(m->stripes);
        return DSM_ERR_MEM;
    }

    /* Doble mapeo con memfd_create */
    m->memfd = memfd_create("dsm-shared", MFD_CLOEXEC | MFD_ALLOW_SEALING);
    if (m->memfd < 0) {
        free(m->backing);
        free(m->local);
        free(m->stripes);
        return DSM_ERR_MEM;
    }

    if (ftruncate(m->memfd, (off_t)m->view_size) < 0) {
        close(m->memfd);
        free(m->backing);
        free(m->local);
        free(m->stripes);
        return DSM_ERR_MEM;
    }

    fcntl(m->memfd, F_ADD_SEALS, F_SEAL_SHRINK | F_SEAL_GROW);

    /* 1. Vista global para la aplicación en BASE_ADDR */
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
        close(m->memfd);
        free(m->backing);
        free(m->local);
        free(m->stripes);
        return DSM_ERR_MEM;
    }

    /* 2. Alias de red con permisos RW constantes */
    m->alias = mmap(NULL, m->view_size, PROT_READ | PROT_WRITE,
                    MAP_SHARED, m->memfd, 0);
    if (m->alias == MAP_FAILED) {
        DSM_LOG_ERR("mmap de alias RW fallo");
        munmap(m->view, m->view_size);
        close(m->memfd);
        free(m->backing);
        free(m->local);
        free(m->stripes);
        return DSM_ERR_MEM;
    }

    DSM_LOG_INFO("Memoria DSM mapeada: Vista en %p, Alias en %p (%lu MB)",
                 (void *)m->view, (void *)m->alias, (unsigned long)(m->view_size >> 20));
    return DSM_OK;
}

void dsm_mem_destroy(dsm_mem_t *m)
{
    if (m->alias) munmap(m->alias, m->view_size);
    if (m->view) munmap(m->view, m->view_size);
    if (m->memfd >= 0) close(m->memfd);
    if (m->backing) free(m->backing);
    if (m->local) free(m->local);
    if (m->stripes) {
        for (uint32_t i = 0; i < m->nstripes; i++) {
            pthread_mutex_destroy(&m->stripes[i]);
        }
        free(m->stripes);
    }
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

void dsm_mem_install_local(dsm_mem_t *m, uint32_t page)
{
    (void)m;
    (void)page;
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
    (void)m;
    return 0;
}

void dsm_mem_frame_free(dsm_mem_t *m, uint32_t frame)
{
    (void)m;
    (void)frame;
}

pthread_mutex_t *dsm_mem_stripe(dsm_mem_t *m, uint32_t page)
{
    return &m->stripes[page % m->nstripes];
}
