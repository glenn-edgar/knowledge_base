#ifndef KB_STREAM_TABLE_H   
#define KB_STREAM_TABLE_H

#include <libpq-fe.h>
int push_stream_data(PGconn *conn, const char *base_table, const char *path, const char *data, int max_retries, double retry_delay, char **error_msg);





#endif
