
#include "system_def.h"
#include "kb_search.h"
#include <libpq-fe.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <uuid/uuid.h>
#include <stdbool.h>
#include "postgres_setup.h"

char error_msg[512];


void individual_status_table(PGconn *conn, const char *base_table, const char *kb, const char *node_name, 
       const char **prop_keys, const char **prop_values, int num_props, const char *node_path){
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        fprintf(stderr, "Failed to create KBQuery\n");
        return;
    }

    int num;
    KBRow *rows = find_status_node_ids(conn,base_table,kb,node_name,prop_keys,prop_values,num_props,node_path,&num);
    if (!rows) {
        fprintf(stderr, "No nodes found matching path parameters\n");
        kb_query_free(q);
        return;
    }
    printf("number of rows: %d\n", num);
    for (int i = 0; i < num; i++) {
        printf("id: %d\n", rows[i].id);
        printf("knowledge_base: %s\n", rows[i].knowledge_base);
        printf("label: %s\n", rows[i].label);
        printf("name: %s\n", rows[i].name);
        printf("properties: %s\n", rows[i].properties);
        printf("data: %s\n", rows[i].data);
        printf("has_link: %d\n", rows[i].has_link);
        printf("has_link_mount: %d\n", rows[i].has_link_mount);
        printf("path: %s\n", rows[i].path);
    }
    kb_rows_free(rows, num);

    
    kb_query_free(q);
}

void find_status_tables(PGconn *conn, const char *base_table){
  
    printf("-------------------------------- find_status_tables\n");
    printf("-------------------------------- wide open test find all status tables\n");
    individual_status_table(conn, base_table, NULL, NULL, NULL, NULL, 0, NULL);
    printf("-------------------------------- search by kb\n");
    individual_status_table(conn, base_table, "kb1", NULL, NULL, NULL, 0, NULL);
    printf("-------------------------------- search by name\n");
    individual_status_table(conn, base_table, NULL, "info3_status", NULL, NULL, 0, NULL);
    printf("-------------------------------- search by path\n");
    individual_status_table(conn, base_table, NULL, NULL, NULL, NULL, 0, "kb1.header1_link.header1_name.KB_STATUS_FIELD.info2_status");
    printf("-------------------------------- search by property keys and values\n");
    individual_status_table(conn, base_table, NULL, NULL,(const char *[]){"prop3","description"}, (const char *[]){"val3","info3_status_description"}, 2,NULL);

}

void find_stream_tables(PGconn *conn, const char *base_table){
    printf("-------------------------------- find_stream_tables\n");
    printf("-------------------------------- wide open test find all stream tables\n");
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        fprintf(stderr, "Failed to create KBQuery\n");
        return;
    }

    int num;
    KBRow *rows = find_stream_ids(conn,base_table,NULL,NULL,NULL,NULL,0,NULL,&num);
    if (!rows) {
        fprintf(stderr, "No nodes found matching path parameters\n");
        kb_query_free(q);
        return;
    }
    printf("number of rows: %d\n", num);
    for (int i = 0; i < num; i++) {
        printf("id: %d\n", rows[i].id);
        printf("knowledge_base: %s\n", rows[i].knowledge_base);
        printf("label: %s\n", rows[i].label);
        printf("name: %s\n", rows[i].name);
        printf("properties: %s\n", rows[i].properties);
        printf("data: %s\n", rows[i].data);
        printf("has_link: %d\n", rows[i].has_link);
        printf("has_link_mount: %d\n", rows[i].has_link_mount);
        printf("path: %s\n", rows[i].path);
    }
    kb_rows_free(rows, num);

    
    kb_query_free(q);
}
void find_job_tables(PGconn *conn, const char *base_table){
    printf("-------------------------------- find_job_tables\n");
    printf("-------------------------------- wide open test find all job tables\n");
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        fprintf(stderr, "Failed to create KBQuery\n");
        return;
    }

    int num;
    KBRow *rows = find_job_ids(conn,base_table,NULL,NULL,NULL,NULL,0,NULL,&num);
    if (!rows) {
        fprintf(stderr, "No nodes found matching path parameters\n");
        kb_query_free(q);
        return;
    }
    printf("number of rows: %d\n", num);
    for (int i = 0; i < num; i++) {
        printf("id: %d\n", rows[i].id);
        printf("knowledge_base: %s\n", rows[i].knowledge_base);
        printf("label: %s\n", rows[i].label);
        printf("name: %s\n", rows[i].name);
        printf("properties: %s\n", rows[i].properties);
        printf("data: %s\n", rows[i].data);
        printf("has_link: %d\n", rows[i].has_link);
        printf("has_link_mount: %d\n", rows[i].has_link_mount);
        printf("path: %s\n", rows[i].path);
    }
    kb_rows_free(rows, num);

    
    kb_query_free(q);
}
void find_rpc_server_tables(PGconn *conn, const char *base_table){
    printf("-------------------------------- find_rpc_server_tables\n");
    printf("-------------------------------- wide open test find all rpc server tables\n");
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        fprintf(stderr, "Failed to create KBQuery\n");
        return;
    }

    int num;
    KBRow *rows = find_rpc_server_ids(conn,base_table,NULL,NULL,NULL,NULL,0,NULL,&num);
    if (!rows) {
        fprintf(stderr, "No nodes found matching path parameters\n");
        kb_query_free(q);
        return;
    }
    printf("number of rows: %d\n", num);
    for (int i = 0; i < num; i++) {
        printf("id: %d\n", rows[i].id);
        printf("knowledge_base: %s\n", rows[i].knowledge_base);
        printf("label: %s\n", rows[i].label);
        printf("name: %s\n", rows[i].name);
        printf("properties: %s\n", rows[i].properties);
        printf("data: %s\n", rows[i].data);
        printf("has_link: %d\n", rows[i].has_link);
        printf("has_link_mount: %d\n", rows[i].has_link_mount);
        printf("path: %s\n", rows[i].path);
    }
    kb_rows_free(rows, num);

    
    kb_query_free(q);
}
void find_rpc_client_tables(PGconn *conn, const char *base_table){
    printf("-------------------------------- find_rpc_client_tables\n");
    printf("-------------------------------- wide open test find all rpc client tables\n");
    KBQuery *q = kb_query_new(base_table);
    if (!q) {
        fprintf(stderr, "Failed to create KBQuery\n");
        return;
    }

    int num;
    KBRow *rows = find_rpc_client_ids(conn,base_table,NULL,NULL,NULL,NULL,0,NULL,&num);
    if (!rows) {
        fprintf(stderr, "No nodes found matching path parameters\n");
        kb_query_free(q);
        return;
    }
    printf("number of rows: %d\n", num);
    for (int i = 0; i < num; i++) {
        printf("id: %d\n", rows[i].id);
        printf("knowledge_base: %s\n", rows[i].knowledge_base);
        printf("label: %s\n", rows[i].label);
        printf("name: %s\n", rows[i].name);
        printf("properties: %s\n", rows[i].properties);
        printf("data: %s\n", rows[i].data);
        printf("has_link: %d\n", rows[i].has_link);
        printf("has_link_mount: %d\n", rows[i].has_link_mount);
        printf("path: %s\n", rows[i].path);
    }
    kb_rows_free(rows, num);

    
    kb_query_free(q);
}




int main(void){
    char password[256];
    printf("Enter password: "); 
    fgets(password, sizeof(password), stdin); 

    PGconn *conn = create_pg_connection("knowledge_base", "gedgar", password, "localhost", "5432");
    if (!conn) {
        fprintf(stderr, "Failed to create PostgreSQL connection\n");
        return 1;
    }
    find_status_tables(conn, "knowledge_base");
    find_stream_tables(conn, "knowledge_base");
    find_job_tables(conn, "knowledge_base");
    find_rpc_server_tables(conn, "knowledge_base");
    find_rpc_client_tables(conn, "knowledge_base");
    
}