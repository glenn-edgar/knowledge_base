#ifndef POSTGRES_SETUP_H
#define POSTGRES_SETUP_H
#include "system_def.h"
#ifndef __MAIN__
#include <libpq-fe.h> // PostgreSQL library
PGconn *create_pg_connection(const char *dbname, const char *user, const char *password, const char *host, const char *port);
#endif


#endif

