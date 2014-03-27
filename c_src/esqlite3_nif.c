/*
 * Copyright 2011, 2012, 2013 Maas-Maarten Zeeman
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

/*
 * sqlite3_nif -- an erlang sqlite nif.
*/

#include <erl_nif.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>
#ifndef  _WIN32
#include <unistd.h>
#endif
#include <fcntl.h>

#include "queue.h"

#ifdef __APPLE__
// Directly include sqlite3.c
// This way we are sure the included version of sqlite3 is actually used.
// If we were to just include "sqlite3.h" OSX would actually use /usr/lib/libsqlite3.dylib
#define SQLITE_API static 
#define SQLITE_EXTERN static 
#include "sqlite3.c" 
#else
#include "sqlite3.h"
#endif

#define MAX_ATOM_LENGTH 255 /* from atom.h, not exposed in erlang include */
#define MAX_PATHNAME 512 /* unfortunately not in sqlite.h. */

static ErlNifResourceType *esqlite_connection_type = NULL;
static ErlNifResourceType *esqlite_statement_type = NULL;

typedef struct {
    queue *commands;
    unsigned int dbcount;
    unsigned int inactivity;
    ErlNifTid tid;
    int alive;
    unsigned int index;
} esqlite_thread;
int g_nthreads;
esqlite_thread* g_threads;

ErlNifUInt64 g_dbcount = 0;
ErlNifMutex *g_dbcount_mutex = NULL;

/* database connection context */
typedef struct {
    unsigned int thread;
    sqlite3 *db;
    char open;
    int wal_prev_pages;
    int wal_pages;
} esqlite_connection;

/* prepared statement */
typedef struct {
    unsigned int thread;
    sqlite3_stmt *statement;
} esqlite_statement;


typedef enum {
    cmd_unknown,
    cmd_open,
    cmd_exec,
    cmd_exec_script,
    cmd_prepare,
    cmd_bind,
    cmd_step,
    cmd_column_names,
    cmd_close,
    cmd_stop
} command_type;

typedef struct {
    command_type type;

    ErlNifEnv *env;
    ERL_NIF_TERM ref; 
    ErlNifPid pid;
    ERL_NIF_TERM arg;
    ERL_NIF_TERM arg1;
    sqlite3_stmt *stmt;
    esqlite_connection *conn;
    // only used for close
    sqlite3 *db;
} esqlite_command;

ERL_NIF_TERM atom_ok;
ERL_NIF_TERM atom_false;
ERL_NIF_TERM atom_error;
ERL_NIF_TERM atom_rows;
ERL_NIF_TERM atom_columns;
ERL_NIF_TERM atom_undefined;
ERL_NIF_TERM atom_rowid;
ERL_NIF_TERM atom_changes;
static ERL_NIF_TERM make_cell(ErlNifEnv *env, sqlite3_stmt *statement, unsigned int i);
static ERL_NIF_TERM push_command(unsigned int thread, esqlite_command *cmd);
static ERL_NIF_TERM make_binary(ErlNifEnv *env, const void *bytes, unsigned int size);

static ERL_NIF_TERM 
make_atom(ErlNifEnv *env, const char *atom_name) 
{
    ERL_NIF_TERM atom;
  
    if(enif_make_existing_atom(env, atom_name, &atom, ERL_NIF_LATIN1)) 
	   return atom;

    return enif_make_atom(env, atom_name);
}

static ERL_NIF_TERM 
make_ok_tuple(ErlNifEnv *env, ERL_NIF_TERM value) 
{
    return enif_make_tuple2(env, atom_ok, value);
}

static ERL_NIF_TERM 
make_error_tuple(ErlNifEnv *env, const char *reason)
{
    return enif_make_tuple2(env, atom_error, make_atom(env, reason));
}

static ERL_NIF_TERM 
make_row_tuple(ErlNifEnv *env, ERL_NIF_TERM value) 
{
    return enif_make_tuple2(env, make_atom(env, "row"), value);
}


int wal_hook(void *data,sqlite3* db,const char* nm,int npages)
{
    esqlite_connection *conn = (esqlite_connection *) data;
    conn->wal_prev_pages = conn->wal_pages;
    conn->wal_pages = npages;
    return SQLITE_OK;
}

static const char *
get_sqlite3_return_code_msg(int r)
{
    switch(r) {
    case SQLITE_OK: return "ok";
    case SQLITE_ERROR : return "sqlite_error";
    case SQLITE_INTERNAL: return "internal";
    case SQLITE_PERM: return "perm"; 
    case SQLITE_ABORT: return "abort"; 
    case SQLITE_BUSY: return "busy";
    case SQLITE_LOCKED: return  "locked";
    case SQLITE_NOMEM: return  "nomem";
    case SQLITE_READONLY: return  "readonly";
    case SQLITE_INTERRUPT: return  "interrupt";
    case SQLITE_IOERR: return  "ioerror";
    case SQLITE_CORRUPT: return  "corrupt";
    case SQLITE_NOTFOUND: return  "notfound";
    case SQLITE_FULL: return  "full";
    case SQLITE_CANTOPEN: return  "cantopen";
    case SQLITE_PROTOCOL: return  "protocol";
    case SQLITE_EMPTY: return  "empty";
    case SQLITE_SCHEMA: return  "schema";
    case SQLITE_TOOBIG: return  "toobig";
    case SQLITE_CONSTRAINT: return  "constraint";
    case SQLITE_MISMATCH: return  "mismatch";
    case SQLITE_MISUSE: return  "misuse";
    case SQLITE_NOLFS: return  "nolfs";
    case SQLITE_AUTH: return  "auth";
    case SQLITE_FORMAT: return  "format";
    case SQLITE_RANGE: return  "range";
    case SQLITE_NOTADB: return  "notadb";
    case SQLITE_ROW: return  "row";
    case SQLITE_DONE: return  "done";
    }
    return  "unknown";
}

static const char *
get_sqlite3_error_msg(int error_code, sqlite3 *db)
{
    if(error_code == SQLITE_MISUSE) 
        return "Sqlite3 was invoked incorrectly.";

    return sqlite3_errmsg(db); 
}

static ERL_NIF_TERM
make_sqlite3_error_tuple(ErlNifEnv *env,const char* calledfrom, int error_code, sqlite3 *db) 
{
    const char *error_code_msg = get_sqlite3_return_code_msg(error_code);
    const char *msg = get_sqlite3_error_msg(error_code, db);
    
    return enif_make_tuple2(env, atom_error, 
        enif_make_tuple3(env, enif_make_string(env,calledfrom,ERL_NIF_LATIN1),
                              make_atom(env, error_code_msg), 
                              enif_make_string(env, msg, ERL_NIF_LATIN1)));
}

static void
command_destroy(void *obj) 
{
    esqlite_command *cmd = (esqlite_command *) obj;

    if(cmd->env != NULL) 
       enif_free_env(cmd->env);

    enif_free(cmd);
}

static esqlite_command *
command_create() 
{
    esqlite_command *cmd = (esqlite_command *) enif_alloc(sizeof(esqlite_command));
    if(cmd == NULL)
       return NULL;

    cmd->env = enif_alloc_env();
    if(cmd->env == NULL) {
        command_destroy(cmd);
        return NULL;
    }

    cmd->type = cmd_unknown;
    cmd->ref = 0;
    cmd->arg = 0;
    cmd->stmt = NULL;
    cmd->conn = NULL;
    cmd->db = NULL;

    return cmd;
}


/*
 *
 */
static void 
destruct_esqlite_connection(ErlNifEnv *env, void *arg)
{
    esqlite_connection *db = (esqlite_connection *) arg;
    if (db->open)
    {
        esqlite_command *cmd = command_create();
  
        /* Send the stop command 
         */
        cmd->type = cmd_close;
        cmd->db = db->db;
        cmd->ref = 0;

        push_command(db->thread, cmd);
    }
}

static void
destruct_esqlite_statement(ErlNifEnv *env, void *arg)
{
    esqlite_statement *stmt = (esqlite_statement *) arg;

    if(stmt->statement) {
	   sqlite3_finalize(stmt->statement);
	   stmt->statement = NULL;
    }
}

static ERL_NIF_TERM
do_open(esqlite_command *cmd, esqlite_thread *thread) 
{
    char filename[MAX_PATHNAME];
    unsigned int size;
    int rc;
    ERL_NIF_TERM error;

    size = enif_get_string(cmd->env, cmd->arg, filename, MAX_PATHNAME, ERL_NIF_LATIN1);
    if(size <= 0) 
        return make_error_tuple(cmd->env, "invalid_filename");

    /* Open the database. 
     */
    rc = sqlite3_open(filename, &(cmd->conn->db));
    if(rc != SQLITE_OK) {
	    error = make_sqlite3_error_tuple(cmd->env, "sqlite3_open", rc, cmd->conn->db);
	    sqlite3_close(cmd->conn->db);

	    return error;
    }
    cmd->conn->wal_prev_pages = cmd->conn->wal_pages = 0;
    sqlite3_wal_hook(cmd->conn->db,wal_hook,cmd->conn);
    cmd->conn->open = 1;

    if (cmd->arg1 == 0)
    {
        enif_release_resource(cmd->conn);
        return atom_ok;
    }
    else
        return 0;
}

/* 
 */
static ERL_NIF_TERM
do_exec(esqlite_command *cmd, esqlite_thread *thread)
{
    ErlNifBinary bin;
    int rc;

    enif_inspect_iolist_as_binary(cmd->env, cmd->arg, &bin);

    rc = sqlite3_exec(cmd->conn->db, (char *)bin.data, NULL, NULL, NULL);
    if(rc != SQLITE_OK)
	    return make_sqlite3_error_tuple(cmd->env,"sqlite3_exec in do_exec", rc, cmd->conn->db);
    enif_release_resource(cmd->conn);

    return atom_ok;
}

/* 
 */
static ERL_NIF_TERM
do_exec_script(esqlite_command *cmd, esqlite_thread *thread)
{
    ErlNifBinary bin;
    int rc = 0,i;
    unsigned int rowcount = 0;
    sqlite3_stmt *statement;
    const char *readpoint;
    const char *end;
    int column_count;
    ERL_NIF_TERM *array;
    ERL_NIF_TERM column_names;
    ERL_NIF_TERM results;
    char skip = 0;
    int statementlen = 0;
    ERL_NIF_TERM rows;

    if (!enif_inspect_iolist_as_binary(cmd->env, cmd->arg, &bin))
        return make_error_tuple(cmd->env, "not iolist");
    end = (char*)bin.data + bin.size;
    readpoint = (char*)bin.data;
    results = enif_make_list(cmd->env,0);
    

    while (readpoint < end)
    {
        if (readpoint[0] == '$')
        {
            skip = 1;
        }
        else
            skip = 0;
        statementlen = end-readpoint;
        rc = sqlite3_prepare_v2(cmd->conn->db, (char *)(readpoint+skip), statementlen, &(statement), &readpoint);
        if(rc != SQLITE_OK)
        {
            rc = SQLITE_ERROR;
            break;
        }
         
        column_count = sqlite3_column_count(statement);
        array = (ERL_NIF_TERM *)malloc(sizeof(ERL_NIF_TERM) * column_count);

        for(i = 0; i < column_count; i++) 
        {
            const char* cname = sqlite3_column_name(statement, i);
            array[i] = make_binary(cmd->env, cname,strlen(cname));
        }

        column_names = enif_make_tuple_from_array(cmd->env, array, column_count);
        free(array);
                
        rows = enif_make_list(cmd->env,0);
        rowcount = 0;
        while ((rc = sqlite3_step(statement)) == SQLITE_ROW)
        {
            ERL_NIF_TERM *array = (ERL_NIF_TERM*)malloc(sizeof(ERL_NIF_TERM)*column_count);

            for(i = 0; i < column_count; i++) 
                array[i] = make_cell(cmd->env, statement, i);

            rows = enif_make_list_cell(cmd->env, enif_make_tuple_from_array(cmd->env, array, column_count), rows);
            free(array);
            rowcount++;
        }
        if (rc == SQLITE_ERROR)
            break;
        
        if (skip == 0 && (rowcount > 0 || column_count > 0))
            results = enif_make_list_cell(cmd->env, enif_make_list2(cmd->env,enif_make_tuple2(cmd->env,atom_columns,column_names),
                                                                      enif_make_tuple2(cmd->env,atom_rows,rows)), 
                                    results);
        else if (skip == 0 && statementlen > 6)
        {
            results = enif_make_list_cell(cmd->env, enif_make_tuple3(cmd->env,atom_changes,
                                             enif_make_int64(cmd->env,sqlite3_last_insert_rowid(cmd->conn->db)), 
                                            enif_make_int(cmd->env,sqlite3_changes(cmd->conn->db))), 
                                            results);

            // const char* sql = sqlite3_sql(statement);
            // if ((sql[0] == 'i' || sql[0] == 'I') &&
            //     (sql[1] == 'n' || sql[1] == 'N') &&
            //     (sql[2] == 's' || sql[2] == 'S') &&
            //     (sql[3] == 'e' || sql[3] == 'E') &&
            //     (sql[4] == 'r' || sql[4] == 'R') &&
            //     (sql[5] == 't' || sql[5] == 'T') &&
            //     (sql[6] == ' '))
            // {
            //     results = enif_make_list_cell(cmd->env, enif_make_tuple2(cmd->env,atom_rowid,
            //                                  enif_make_int64(cmd->env,sqlite3_last_insert_rowid(cmd->conn->db))), 
            //                                 results);
            // }
            // else
            // {
            //     results = enif_make_list_cell(cmd->env, atom_ok, results);
            // }
        }
        sqlite3_finalize(statement);
    }
    
    enif_release_resource(cmd->conn);
    if (rc == SQLITE_ERROR)
        return make_sqlite3_error_tuple(cmd->env, "exec_script", rc, cmd->conn->db);
    else
    {
        return make_ok_tuple(cmd->env,results);
    }
}

/*
 */
static ERL_NIF_TERM
do_prepare(esqlite_command *cmd, esqlite_thread *thread)
{
    ErlNifBinary bin;
    esqlite_statement *stmt;
    ERL_NIF_TERM esqlite_stmt;
    const char *tail;
    int rc = 0;

    enif_inspect_iolist_as_binary(cmd->env, cmd->arg, &bin);

    stmt = enif_alloc_resource(esqlite_statement_type, sizeof(esqlite_statement));
    if(!stmt) 
	    return make_error_tuple(cmd->env, "no_memory");

    rc = sqlite3_prepare_v2(cmd->conn->db, (char *) bin.data, bin.size, &(stmt->statement), &tail);
    if(rc != SQLITE_OK)
	    return make_sqlite3_error_tuple(cmd->env, "sqlite3_prepare_v2 in do_prepare", rc, cmd->conn->db);

    enif_release_resource(cmd->conn);
    stmt->thread = cmd->conn->thread;

    esqlite_stmt = enif_make_resource(cmd->env, stmt);
    enif_release_resource(stmt);

    return make_ok_tuple(cmd->env, esqlite_stmt);
}

static int
bind_cell(ErlNifEnv *env, const ERL_NIF_TERM cell, sqlite3_stmt *stmt, unsigned int i)
{
    int the_int;
    ErlNifSInt64 the_long_int;
    double the_double;
    char the_atom[MAX_ATOM_LENGTH+1];
    ErlNifBinary the_blob;
    int arity;
    const ERL_NIF_TERM* tuple;

    if(enif_get_int(env, cell, &the_int)) 
        return sqlite3_bind_int(stmt, i, the_int);

    if(enif_get_int64(env, cell, &the_long_int)) 
        return sqlite3_bind_int64(stmt, i, the_long_int);

    if(enif_get_double(env, cell, &the_double)) 
        return sqlite3_bind_double(stmt, i, the_double);

    if(enif_get_atom(env, cell, the_atom, sizeof(the_atom), ERL_NIF_LATIN1)) {
        if(strcmp("undefined", the_atom) == 0) {
           return sqlite3_bind_null(stmt, i);
        }

        return sqlite3_bind_text(stmt, i, the_atom, strlen(the_atom), SQLITE_TRANSIENT);
    }

    /* Bind as text assume it is utf-8 encoded text */
    if(enif_inspect_iolist_as_binary(env, cell, &the_blob))
        return sqlite3_bind_text(stmt, i, (char *) the_blob.data, the_blob.size, SQLITE_TRANSIENT);

    /* Check for blob tuple */
    if(enif_get_tuple(env, cell, &arity, &tuple)) {
        if(arity != 2) 
            return -1;

        /* length 2! */
        if(enif_get_atom(env, tuple[0], the_atom, sizeof(the_atom), ERL_NIF_LATIN1)) {
            /* its a blob... */
            if(0 == strncmp("blob", the_atom, strlen("blob"))) {
                /* with a iolist as argument */
                if(enif_inspect_iolist_as_binary(env, tuple[1], &the_blob)) {
                    /* kaboom... get the blob */
                    return sqlite3_bind_blob(stmt, i, the_blob.data, the_blob.size, SQLITE_TRANSIENT);
                }
            }
        }
    }

    return -1;
}

static ERL_NIF_TERM
do_bind(esqlite_command *cmd, esqlite_thread *thread)
{
    int parameter_count = sqlite3_bind_parameter_count(cmd->stmt);
    int i, is_list, r;
    ERL_NIF_TERM list, head, tail;
    unsigned int list_length;

    is_list = enif_get_list_length(cmd->env, cmd->arg, &list_length);
    if(!is_list) 
	    return make_error_tuple(cmd->env, "bad_arg_list");
    if(parameter_count != list_length) 
	    return make_error_tuple(cmd->env, "args_wrong_length");

    sqlite3_reset(cmd->stmt);
     
    list = cmd->arg;
    for(i=0; i < list_length; i++) {
	    enif_get_list_cell(cmd->env, list, &head, &tail);
	    r = bind_cell(cmd->env, head, cmd->stmt, i+1);
	    if(r == -1) 
	        return make_error_tuple(cmd->env, "wrong_type");
	    if(r != SQLITE_OK)
	        return make_error_tuple(cmd->env, "sqlite_not_ok");
	    list = tail;
    }
     
    return atom_ok;
}

static ERL_NIF_TERM
make_binary(ErlNifEnv *env, const void *bytes, unsigned int size) 
{
    ErlNifBinary blob;
    ERL_NIF_TERM term;

    if(!enif_alloc_binary(size, &blob)) {
	    /* TODO: fix this */
	    return atom_error;
    }

    memcpy(blob.data, bytes, size);
    term = enif_make_binary(env, &blob);
    enif_release_binary(&blob);

    return term;
}

static ERL_NIF_TERM
make_cell(ErlNifEnv *env, sqlite3_stmt *statement, unsigned int i)
{
    int type = sqlite3_column_type(statement, i);
     
    switch(type) {
    case SQLITE_INTEGER:
	    return enif_make_int64(env, sqlite3_column_int64(statement, i));
    case SQLITE_FLOAT:
	    return enif_make_double(env, sqlite3_column_double(statement, i));
    case SQLITE_BLOB:
        return enif_make_tuple2(env, make_atom(env, "blob"), 
                make_binary(env, sqlite3_column_blob(statement, i), 
                    sqlite3_column_bytes(statement, i)));
    case SQLITE_NULL:
	    return atom_undefined;
    case SQLITE_TEXT:
	    return make_binary(env, sqlite3_column_text(statement, i), 
            sqlite3_column_bytes(statement, i));
    default:
	    return make_atom(env, "should_not_happen");
    }
}

static ERL_NIF_TERM
make_row(ErlNifEnv *env, sqlite3_stmt *statement) 
{
    int i, size;
    ERL_NIF_TERM *array;
    ERL_NIF_TERM row;
     
    size = sqlite3_column_count(statement);
    array = (ERL_NIF_TERM *) malloc(sizeof(ERL_NIF_TERM)*size);

    if(!array) 
	    return make_error_tuple(env, "no_memory");

    for(i = 0; i < size; i++) 
	    array[i] = make_cell(env, statement, i);

    row = make_row_tuple(env, enif_make_tuple_from_array(env, array, size));
    free(array);
    return row;
}

static ERL_NIF_TERM
do_step(esqlite_command *cmd, esqlite_thread *thread)
{
    int rc = sqlite3_step(cmd->stmt);

    if(rc == SQLITE_ROW) 
        return make_row(cmd->env, cmd->stmt);
    if(rc == SQLITE_DONE) 
	    return make_atom(cmd->env, "$done");
    if(rc == SQLITE_BUSY)
	    return make_atom(cmd->env, "$busy");

    if(rc == SQLITE_ERROR)
        return make_error_tuple(cmd->env, "step_sqlite_error");
    if(rc == SQLITE_MISUSE)
        return make_error_tuple(cmd->env, "misuse");

    return make_error_tuple(cmd->env, "unexpected_return_value");
}

static ERL_NIF_TERM
do_column_names(esqlite_command *cmd, esqlite_thread *thread)
{
    int i, size;
    const char *name;
    ERL_NIF_TERM *array;
    ERL_NIF_TERM column_names;
     
    size = sqlite3_column_count(cmd->stmt);
    array = (ERL_NIF_TERM *) malloc(sizeof(ERL_NIF_TERM) * size);
     
    if(!array)
	    return make_error_tuple(cmd->env, "no_memory");

    for(i = 0; i < size; i++) {
	    name = sqlite3_column_name(cmd->stmt, i);
	    array[i] = make_atom(cmd->env, name);
    }

    column_names = enif_make_tuple_from_array(cmd->env, array, size);
    free(array);
    return column_names;
}

static ERL_NIF_TERM
do_close(esqlite_command *cmd,esqlite_thread *thread)
{
    ERL_NIF_TERM ret;
    int rc;
    
    rc = sqlite3_close(cmd->db);
    if(rc != SQLITE_OK)
    {
        ret = make_sqlite3_error_tuple(cmd->env,"sqlite3_close in do_close",rc,cmd->db);
    }
    else
    {
        if (cmd->conn != NULL)
        {
            cmd->conn->open = 0;
            enif_release_resource(cmd->conn);
        }
        ret = atom_ok;
    }

    return ret;
}

static ERL_NIF_TERM
evaluate_command(esqlite_command *cmd,esqlite_thread *thread)
{
    switch(cmd->type) {
    case cmd_open:
    {
        ERL_NIF_TERM res = do_open(cmd,thread);
        if (res != 0 || cmd->conn->open == 0)
            return res;
        else
        {
            cmd->arg = cmd->arg1;
            return do_exec_script(cmd,thread);
        }
    }
    case cmd_exec:
	    return do_exec(cmd,thread);
    case cmd_exec_script:
        return do_exec_script(cmd,thread);
    case cmd_prepare:
	    return do_prepare(cmd,thread);
    case cmd_step:
	    return do_step(cmd,thread);
    case cmd_bind:
	    return do_bind(cmd,thread);
    case cmd_column_names:
	    return do_column_names(cmd,thread);
    case cmd_close:
	    return do_close(cmd,thread);
    case cmd_unknown:
        return atom_ok;
    default:
	    return make_error_tuple(cmd->env, "invalid_command");
    }
}

// esqlite_connection *conn
static ERL_NIF_TERM
push_command(unsigned int thread, esqlite_command *cmd) 
{
    if(!queue_push(g_threads[thread].commands, cmd)) 
    {
        return make_error_tuple(cmd->env, "command_push_failed");
    }

    return atom_ok;
}

static ERL_NIF_TERM
make_answer(esqlite_command *cmd, ERL_NIF_TERM answer)
{
    return enif_make_tuple2(cmd->env, cmd->ref, answer);
}

static void *
esqlite_thread_func(void *arg)
{
    esqlite_thread* data = (esqlite_thread*)arg;
    esqlite_command *cmd;
    data->alive = 1;

    while(1) 
    {
        cmd = queue_pop(data->commands);
        
        if (cmd->type == cmd_stop)
            break;
        else
        {
            if (cmd->type == cmd_close && cmd->ref == 0)
                evaluate_command(cmd,data);
            else
                enif_send(NULL, &cmd->pid, cmd->env, make_answer(cmd, evaluate_command(cmd,data)));
        }
        
	    command_destroy(cmd);
    }
  
    data->alive = 0;
    return NULL;
}

static ERL_NIF_TERM
parse_helper(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary bin;
    unsigned int offset = 0;
    char instr = 0;

    if (argc != 2)
        return enif_make_badarg(env);

    if (!enif_inspect_binary(env, argv[0], &bin))
        return enif_make_badarg(env);
    if (!enif_get_uint(env, argv[1], &offset)) 
        return enif_make_badarg(env);

    for (;offset < bin.size;offset++)
    {
        if (bin.data[offset] == '\'' || bin.data[offset] == '`')
            instr = !instr;
        // If ; outside of string, return offset
        else if (bin.data[offset] == ';' && !instr)
        {
            return enif_make_uint(env,offset);
        }
        // If {{ return offset
        else if (bin.data[offset] == '{' && offset+1 < bin.size)
        {
            if (bin.data[offset+1] == '{')
                return enif_make_uint(env,offset);
        }
        else if (bin.data[offset] == '/' && offset+1 < bin.size && !instr)
        {
            if (bin.data[offset+1] == '*')
                return enif_make_uint(env,offset);
        }
    }

    return atom_ok;
}


/* 
 * Open the database
 */
static ERL_NIF_TERM
esqlite_open(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_command *cmd = NULL;
    ErlNifPid pid;
    esqlite_connection* conn;
    ERL_NIF_TERM db_conn;
     
    if(!(argc == 4 || argc == 5)) 
	    return enif_make_badarg(env);     
    if(!enif_is_ref(env, argv[0])) 
	    return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[1], &pid)) 
	    return make_error_tuple(env, "invalid_pid");

    conn = enif_alloc_resource(esqlite_connection_type, sizeof(esqlite_connection));
    if(!enif_get_uint(env, argv[3], &(conn->thread))) 
        return make_error_tuple(env, "invalid_thread_number");
    conn->thread %= g_nthreads;
    conn->open = 0;

    if(!conn) 
        return make_error_tuple(env, "no_memory");

    cmd = command_create();
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_open;
    cmd->ref = enif_make_copy(cmd->env, argv[0]);
    cmd->pid = pid;
    cmd->arg = enif_make_copy(cmd->env, argv[2]);
    if (argc == 5)
        cmd->arg1 = enif_make_copy(cmd->env, argv[4]);
    else
        cmd->arg1 = 0;
    cmd->conn = conn;

    db_conn = enif_make_resource(env, conn);
    return enif_make_tuple2(env,push_command(conn->thread, cmd),db_conn);
}

static ERL_NIF_TERM 
esqlite_wal_pages(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *db;

    if (argc != 1)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &db))
    {
        return enif_make_badarg(env);
    }
    return enif_make_tuple2(env, enif_make_int(env,db->wal_prev_pages),enif_make_int(env,db->wal_pages));
}

/*
 * Execute the sql statement
 */
static ERL_NIF_TERM 
esqlite_exec(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *db;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;
     
    if(argc != 4) 
	    return enif_make_badarg(env);  
    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &db))
    {
        return enif_make_badarg(env);
    }
        
	    
    if(!enif_is_ref(env, argv[1])) 
	    return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[2], &pid)) 
	    return make_error_tuple(env, "invalid_pid"); 

    cmd = command_create();
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");
    
    /* command */
    cmd->type = cmd_exec;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->arg = enif_make_copy(cmd->env, argv[3]);
    cmd->conn = db;
    enif_keep_resource(db);

    return push_command(db->thread, cmd);
}

/*
 * Execute the sql statement
 */
static ERL_NIF_TERM 
esqlite_exec_script(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *db;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;
     
    if(argc != 4) 
        return enif_make_badarg(env);  
    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &db))
        return enif_make_badarg(env);
        
    if(!enif_is_ref(env, argv[1])) 
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[2], &pid)) 
        return make_error_tuple(env, "invalid_pid"); 
    
    cmd = command_create();
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");
     
    /* command */
    cmd->type = cmd_exec_script;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->arg = enif_make_copy(cmd->env, argv[3]);
    cmd->conn = db;
    enif_keep_resource(db);

    return push_command(db->thread, cmd);
}


/*
 * Prepare the sql statement
 */
static ERL_NIF_TERM 
esqlite_prepare(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *conn;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 4) 
	    return enif_make_badarg(env);
    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &conn))
	    return enif_make_badarg(env);
    if(!enif_is_ref(env, argv[1])) 
	    return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[2], &pid)) 
	    return make_error_tuple(env, "invalid_pid"); 

    cmd = command_create();
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_prepare;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->arg = enif_make_copy(cmd->env, argv[3]);
    cmd->conn = conn;
    enif_keep_resource(conn);

    return push_command(conn->thread, cmd);
}

/*
 * Bind a variable to a prepared statement
 */
static ERL_NIF_TERM 
esqlite_bind(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_statement *stmt;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 4) 
	    return enif_make_badarg(env);
    if(!enif_get_resource(env, argv[0], esqlite_statement_type, (void **) &stmt))
	    return enif_make_badarg(env);
    if(!enif_is_ref(env, argv[1])) 
	    return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[2], &pid)) 
	    return make_error_tuple(env, "invalid_pid"); 

    cmd = command_create();
    if(!cmd) 
	    return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_bind;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->stmt = stmt->statement;
    cmd->arg = enif_make_copy(cmd->env, argv[3]);

    return push_command(stmt->thread, cmd);
}

/*
 * Step to a prepared statement
 */
static ERL_NIF_TERM 
esqlite_step(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_statement *stmt;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 3) 
	    return enif_make_badarg(env);
    if(!enif_get_resource(env, argv[0], esqlite_statement_type, (void **) &stmt))
	    return enif_make_badarg(env);
    if(!enif_is_ref(env, argv[1])) 
	    return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[2], &pid)) 
	    return make_error_tuple(env, "invalid_pid"); 
    if(!stmt->statement) 
	    return make_error_tuple(env, "no_prepared_statement");

    cmd = command_create();
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_step;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->stmt = stmt->statement;

    return push_command(stmt->thread, cmd);
}

static ERL_NIF_TERM 
esqlite_noop(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *conn;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 3) 
        return enif_make_badarg(env);
    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &conn))
        return enif_make_badarg(env);
    if(!enif_is_ref(env, argv[1])) 
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[2], &pid)) 
        return make_error_tuple(env, "invalid_pid"); 

    cmd = command_create();
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_unknown;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;

    return push_command(conn->thread, cmd);
}

/*
 * Step to a prepared statement
 */
static ERL_NIF_TERM 
esqlite_column_names(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_statement *stmt;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(argc != 3) 
	    return enif_make_badarg(env);
    if(!enif_get_resource(env, argv[0], esqlite_statement_type, (void **) &stmt))
	    return enif_make_badarg(env);
    if(!enif_is_ref(env, argv[1])) 
	    return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[2], &pid)) 
	    return make_error_tuple(env, "invalid_pid"); 
    if(!stmt->statement) 
	    return make_error_tuple(env, "no_prepared_statement");

    cmd = command_create();
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_column_names;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->stmt = stmt->statement;

    return push_command(stmt->thread, cmd);
}

/*
 * Close the database
 */
static ERL_NIF_TERM
esqlite_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *conn;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &conn))
	    return enif_make_badarg(env);
    if(!enif_is_ref(env, argv[1])) 
	    return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[2], &pid)) 
	    return make_error_tuple(env, "invalid_pid"); 

    cmd = command_create();
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_close;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->conn = conn;
    cmd->db = conn->db;
    enif_keep_resource(conn);

    return push_command(conn->thread, cmd);
}

/*
 * Load the nif. Initialize some stuff and such
 */
static int 
on_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{
    ErlNifResourceType *rt;
    int i = 0;

    atom_false = enif_make_atom(env,"false");
    atom_ok = enif_make_atom(env,"ok");
    atom_rows = enif_make_atom(env,"rows");
    atom_columns = enif_make_atom(env,"columns");
    atom_error = enif_make_atom(env,"error");
    atom_undefined = enif_make_atom(env,"undefined");
    atom_rowid = enif_make_atom(env,"rowid");
    atom_changes = enif_make_atom(env,"changes");
    
    if (!enif_get_int(env,info,&g_nthreads))
    {
        printf("Unable to read int for nthreads\r\n");
        return -1;
    }
    g_dbcount_mutex = enif_mutex_create("dbcountmutex");
     
    rt = enif_open_resource_type(env, "esqlite3_nif", "esqlite_connection_type", 
				destruct_esqlite_connection, ERL_NIF_RT_CREATE, NULL);
    if(!rt) 
	    return -1;
    esqlite_connection_type = rt;

    rt =  enif_open_resource_type(env, "esqlite3_nif", "esqlite_statement_type",
				   destruct_esqlite_statement, ERL_NIF_RT_CREATE, NULL);
    if(!rt) 
	    return -1;
    esqlite_statement_type = rt;
    
    g_threads = (esqlite_thread*)malloc(sizeof(esqlite_thread)*g_nthreads);
    memset(g_threads,0,sizeof(esqlite_thread)*g_nthreads);
    
    for (i = 0; i < g_nthreads; i++)
    {
        g_threads[i].index = i;
        g_threads[i].commands = queue_create();

        /* Start command processing thread */
        if(enif_thread_create("esqlite_connection", &(g_threads[i].tid), esqlite_thread_func, &(g_threads[i]), NULL) != 0) 
        {
            printf("Unable to create esqlite3 thread\r\n");
            return -1;
        }
    }

    return 0;
}

static void 
on_unload(ErlNifEnv* env, void* priv_data)
{
    int i;
    for (i = 0; i < g_nthreads; i++)
    {
        esqlite_command cmd;
        cmd.type = cmd_stop;
        cmd.env = NULL;
        push_command(i, &cmd);
        enif_thread_join((ErlNifTid)g_threads[i].tid,NULL);
    }
}

static ErlNifFunc nif_funcs[] = {
    {"open", 4, esqlite_open},
    {"open", 5, esqlite_open},
    {"exec", 4, esqlite_exec},
    {"exec_script", 4, esqlite_exec_script},
    {"prepare", 4, esqlite_prepare},
    {"step", 3, esqlite_step},
    {"noop", 3, esqlite_noop},
    // TODO: {"esqlite_bind", 3, esqlite_bind_named},
    {"bind", 4, esqlite_bind},
    {"parse_helper",2,parse_helper},
    {"column_names", 3, esqlite_column_names},
    {"close", 3, esqlite_close},
    {"wal_pages",1,esqlite_wal_pages}
};

ERL_NIF_INIT(esqlite3_nif, nif_funcs, on_load, NULL, NULL, on_unload);
