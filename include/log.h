#ifndef DSM_LOG_H
#define DSM_LOG_H

#include <stdio.h>

enum {
    DSM_LOG_LVL_ERROR = 0,
    DSM_LOG_LVL_WARN  = 1,
    DSM_LOG_LVL_INFO  = 2,
    DSM_LOG_LVL_DEBUG = 3,
    DSM_LOG_LVL_TRACE = 4
};

void dsm_log_set_level(int level);
int  dsm_log_level_from_str(const char *str);
void dsm_log_init_from_env(void);
#if defined(__GNUC__) || defined(__clang__)
__attribute__((format(printf, 4, 5)))
#endif
void dsm_log_write(int level, const char *file, int line, const char *fmt, ...);

#define DSM_LOG_ERR(...)   dsm_log_write(DSM_LOG_LVL_ERROR, __FILE__, __LINE__, __VA_ARGS__)
#define DSM_LOG_WARN(...)  dsm_log_write(DSM_LOG_LVL_WARN,  __FILE__, __LINE__, __VA_ARGS__)
#define DSM_LOG_INFO(...)  dsm_log_write(DSM_LOG_LVL_INFO,  __FILE__, __LINE__, __VA_ARGS__)
#define DSM_LOG_DEBUG(...) dsm_log_write(DSM_LOG_LVL_DEBUG, __FILE__, __LINE__, __VA_ARGS__)
#define DSM_LOG_TRACE(...) dsm_log_write(DSM_LOG_LVL_TRACE, __FILE__, __LINE__, __VA_ARGS__)

#endif /* DSM_LOG_H */
