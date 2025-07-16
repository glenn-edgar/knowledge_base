#ifndef KB_STATUS_TABLE_H
#define KB_STATUS_TABLE_H
#include <libpq-fe.h> // PostgreSQL library

typedef struct {
    PGconn *kb_search; // PostgreSQL connection
    const char *base_table; // Table name
} StatusDataContext;

int get_status_data(StatusDataContext *self, const char *path, char **data_str);
int set_status_data(StatusDataContext *self, const char *path, const char *data, int retry_count, double retry_delay, int *success, char *message);


#endif
