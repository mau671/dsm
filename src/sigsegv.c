#include "sigsegv.h"
#include "dsm_internal.h"
#include <signal.h>
#include <unistd.h>
#include <string.h>
#include <sys/syscall.h>
#include <linux/futex.h>
#include <sys/mman.h>
#include <ucontext.h>
#include <string.h>

#define STACK_SIZE (64 * 1024)

static struct sigaction g_old_sa;
static bool g_sig_installed = false;

/* Ranura de fallo TLS usando initial-exec para evitar malloc en el handler */
static __thread fault_slot_t g_thread_slot __attribute__((tls_model("initial-exec")));
static __thread bool g_thread_attached = false;
static __thread void *g_altstack_buf = NULL;

/* Cola de fallos MPMC acotada para paso al despachador.
 *
 * Varios hilos de aplicacion pueden fallar a la vez, y con mas de un
 * despachador tambien hay varios consumidores, de modo que productor y
 * consumidor reservan su posicion con compare-and-swap y cada celda lleva un
 * contador de secuencia que indica si esta libre para la generacion en curso.
 * La implementacion no toma cerrojos porque push se ejecuta dentro del
 * manejador de SIGSEGV, donde solo son validas las operaciones atomicas. */
typedef struct {
    fault_slot_t *slot;
    _Atomic uint32_t seq;
} fault_cell_t;

static fault_cell_t g_fault_queue[DSM_FAULTQ_CAP];
static _Atomic uint32_t g_fq_head = 0;
static _Atomic uint32_t g_fq_tail = 0;

_Static_assert((DSM_FAULTQ_CAP & (DSM_FAULTQ_CAP - 1)) == 0,
               "DSM_FAULTQ_CAP debe ser potencia de dos");

int faultq_init(void)
{
    atomic_store(&g_fq_head, 0);
    atomic_store(&g_fq_tail, 0);
    for (uint32_t i = 0; i < DSM_FAULTQ_CAP; i++) {
        g_fault_queue[i].slot = NULL;
        atomic_store_explicit(&g_fault_queue[i].seq, i, memory_order_relaxed);
    }
    return 0;
}

bool faultq_push(fault_slot_t *s)
{
    const uint32_t mask = DSM_FAULTQ_CAP - 1;
    uint32_t pos = atomic_load_explicit(&g_fq_tail, memory_order_relaxed);

    for (;;) {
        fault_cell_t *cell = &g_fault_queue[pos & mask];
        uint32_t seq = atomic_load_explicit(&cell->seq, memory_order_acquire);
        int32_t dif = (int32_t)(seq - pos);
        if (dif == 0) {
            if (atomic_compare_exchange_weak_explicit(&g_fq_tail, &pos, pos + 1,
                                                      memory_order_relaxed,
                                                      memory_order_relaxed)) {
                cell->slot = s;
                atomic_store_explicit(&cell->seq, pos + 1, memory_order_release);
                return true;
            }
        } else if (dif < 0) {
            return false; /* cola llena */
        } else {
            pos = atomic_load_explicit(&g_fq_tail, memory_order_relaxed);
        }
    }
}

fault_slot_t *faultq_pop(void)
{
    const uint32_t mask = DSM_FAULTQ_CAP - 1;
    uint32_t pos = atomic_load_explicit(&g_fq_head, memory_order_relaxed);

    for (;;) {
        fault_cell_t *cell = &g_fault_queue[pos & mask];
        uint32_t seq = atomic_load_explicit(&cell->seq, memory_order_acquire);
        int32_t dif = (int32_t)(seq - (pos + 1));
        if (dif == 0) {
            if (atomic_compare_exchange_weak_explicit(&g_fq_head, &pos, pos + 1,
                                                      memory_order_relaxed,
                                                      memory_order_relaxed)) {
                fault_slot_t *s = cell->slot;
                atomic_store_explicit(&cell->seq, pos + mask + 1,
                                      memory_order_release);
                return s;
            }
        } else if (dif < 0) {
            return NULL; /* cola vacia */
        } else {
            pos = atomic_load_explicit(&g_fq_head, memory_order_relaxed);
        }
    }
}

static void sigsegv_handler(int sig, siginfo_t *info, void *ucontext_raw)
{
    (void)sig;
    dsm_ctx_t *ctx = dsm_ctx_get();
    if (!ctx) {
        /* Fuera de control de DSM: restaurar y abortar */
        sigaction(SIGSEGV, &g_old_sa, NULL);
        return;
    }

    uint8_t *addr = (uint8_t *)info->si_addr;
    if (addr < ctx->mem.view || addr >= ctx->mem.view + ctx->mem.view_size) {
        /* Dirección fuera de la vista compartida DSM: fallo de aplicacion real */
        sigaction(SIGSEGV, &g_old_sa, NULL);
        return;
    }

    ucontext_t *uc = (ucontext_t *)ucontext_raw;
    greg_t err_code = uc->uc_mcontext.gregs[REG_ERR];

    uint8_t access = (err_code & 0x2) ? DSM_ACCESS_WRITE : DSM_ACCESS_READ;
    uint32_t page = (uint32_t)((addr - ctx->mem.view) / ctx->mem.page_size);

    fault_slot_t *s = &g_thread_slot;
    s->page   = page;
    s->access = access;
    s->done   = 0;

    if (!faultq_push(s)) {
        _exit(128 + SIGSEGV);
    }
    atomic_fetch_add_explicit(&ctx->ctr.slots_enqueued, 1, memory_order_relaxed);

    if (access == DSM_ACCESS_WRITE) {
        atomic_fetch_add_explicit(&ctx->ctr.write_faults, 1, memory_order_relaxed);
    } else {
        atomic_fetch_add_explicit(&ctx->ctr.read_faults, 1, memory_order_relaxed);
    }

    /* Dormir en futex hasta que el despachador resuelva el permiso */
    while (__atomic_load_n(&s->done, __ATOMIC_ACQUIRE) == 0) {
        syscall(SYS_futex, (int *)&s->done, FUTEX_WAIT, 0, NULL, NULL, 0);
    }
}

int dsm_sig_install(void)
{
    if (g_sig_installed) return DSM_OK;

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_sigaction = sigsegv_handler;
    sa.sa_flags     = SA_SIGINFO | SA_ONSTACK | SA_RESTART;
    sigemptyset(&sa.sa_mask);

    if (sigaction(SIGSEGV, &sa, &g_old_sa) != 0) {
        return DSM_ERR_MEM;
    }
    g_sig_installed = true;
    return DSM_OK;
}

void dsm_sig_restore(void)
{
    if (g_sig_installed) {
        sigaction(SIGSEGV, &g_old_sa, NULL);
        g_sig_installed = false;
    }
}

dsm_status_t dsm_sig_thread_attach(void)
{
    if (g_thread_attached) return DSM_OK;

    /* Pila alternativa para el hilo de 64 KiB */
    void *stack_mem = mmap(NULL, STACK_SIZE, PROT_READ | PROT_WRITE,
                           MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (stack_mem == MAP_FAILED) {
        return DSM_ERR_MEM;
    }

    stack_t ss;
    ss.ss_sp    = stack_mem;
    ss.ss_size  = STACK_SIZE;
    ss.ss_flags = 0;

    if (sigaltstack(&ss, NULL) != 0) {
        munmap(stack_mem, STACK_SIZE);
        return DSM_ERR_MEM;
    }

    g_altstack_buf    = stack_mem;
    g_thread_attached = true;
    return DSM_OK;
}

dsm_status_t dsm_sig_thread_detach(void)
{
    if (!g_thread_attached) return DSM_OK;

    stack_t ss;
    ss.ss_sp    = NULL;
    ss.ss_flags = SS_DISABLE;
    ss.ss_size  = 0;
    sigaltstack(&ss, NULL);

    if (g_altstack_buf) {
        munmap(g_altstack_buf, STACK_SIZE);
        g_altstack_buf = NULL;
    }

    g_thread_attached = false;
    return DSM_OK;
}
