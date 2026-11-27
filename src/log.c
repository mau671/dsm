#include "log.h"
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>

static int g_log_level = DSM_LOG_LVL_INFO;

void dsm_log_set_level(int level)
{
    g_log_level = level;
}

int dsm_log_level_from_str(const char *str)
{
    if (!str) return DSM_LOG_LVL_INFO;
    if (strcasecmp(str, "ERROR") == 0) return DSM_LOG_LVL_ERROR;
    if (strcasecmp(str, "WARN") == 0)  return DSM_LOG_LVL_WARN;
    if (strcasecmp(str, "INFO") == 0)  return DSM_LOG_LVL_INFO;
    if (strcasecmp(str, "DEBUG") == 0) return DSM_LOG_LVL_DEBUG;
    if (strcasecmp(str, "TRACE") == 0) return DSM_LOG_LVL_TRACE;
    return DSM_LOG_LVL_INFO;
}

void dsm_log_init_from_env(void)
{
    const char *env = getenv("DSM_LOG_LEVEL");
    if (env) {
        g_log_level = dsm_log_level_from_str(env);
    }
}

void dsm_log_write(int level, const char *file, int line, const char *fmt, ...)
{
    if (level > g_log_level) {
        return;
    }

    static const char *level_names[] = {
        "error", "aviso", "info", "depuración", "traza"
    };

    struct timeval tv;
    gettimeofday(&tv, NULL);
    struct tm tm_buf;
    localtime_r(&tv.tv_sec, &tm_buf);

    char time_str[32];
    strftime(time_str, sizeof(time_str), "%H:%M:%S", &tm_buf);

    const char *base_file = strrchr(file, '/');
    base_file = base_file ? base_file + 1 : file;

    /* El campo de origen se paddea a un ancho fijo: el nombre del archivo
     * cambia de largo y, sin el relleno, el mensaje empezaría en una columna
     * distinta en cada línea. */
    char origin[64];
    snprintf(origin, sizeof(origin), "[%s:%d]", base_file, line);

    flockfile(stderr);

    fprintf(stderr, "[%s.%03ld] [%-10s] %-26s ",
            time_str, (long)(tv.tv_usec / 1000),
            (level >= 0 && level <= 4) ? level_names[level] : "?????",
            origin);

    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);

    fputc('\n', stderr);
    fflush(stderr);

    funlockfile(stderr);
}
