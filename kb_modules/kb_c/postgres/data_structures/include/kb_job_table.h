#ifndef KB_JOB_TABLE_H
#define KB_JOB_TABLE_H

#include <libpq-fe.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct {
    PGconn *kb_search;
    const char *base_table;
} JobQueueContext;


typedef struct {
    int found;
    int id;
    char *data;
} JobInfo;





int clear_job_queue(JobQueueContext *self, const char *path,  char *message);
int push_job_data(JobQueueContext *self, const char *path, const char *data, int max_retries, double retry_delay,  char *message);
int mark_job_completed(JobQueueContext *self, int job_id, int max_retries, double retry_delay,  char *message);
int peak_job_data(JobQueueContext *self, const char *path, int max_retries, double retry_delay, JobInfo *job_info, char *message);
int get_free_number(JobQueueContext *self, const char *path, int *count, char *message);
int get_queued_number(JobQueueContext *self, const char *path, int *count, char *message);

#endif