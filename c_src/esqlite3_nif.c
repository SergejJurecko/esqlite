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


#ifdef __linux__
#define _GNU_SOURCE 1
#include <sys/mman.h>
#include <dlfcn.h>
#endif

#include <erl_nif.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include <fcntl.h>

#ifndef  _WIN32
#include <sys/time.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/uio.h>
#include <netinet/tcp.h>
#else
#include <winsock2.h>
#include <ws2tcpip.h>
#endif



#include "lz4.h"
#include "queue.h"

// #ifdef __APPLE__
//#ifndef _WIN32
// Directly include sqlite3.c
// This way we are sure the included version of sqlite3 is actually used.
// If we were to just include "sqlite3.h" OSX would actually use /usr/lib/libsqlite3.dylib
#define SQLITE_API static 
#define SQLITE_EXTERN static 
#include "sqlite3.c" 
//#else
//#include "sqlite3.h"
//#endif

#define MAX_ATOM_LENGTH 255 /* from atom.h, not exposed in erlang include */
#define MAX_PATHNAME 512 /* unfortunately not in sqlite.h. */
#define PAGE_BUFF_SIZE 9000
#define MAX_CONNECTIONS 8
#define PACKET_ITEMS 9
#define MAX_STATIC_SQLS 10
#define MAX_PREP_SQLS 100

static ErlNifResourceType *esqlite_connection_type = NULL;
static ErlNifResourceType *esqlite_backup_type = NULL;

char g_static_sqls[MAX_STATIC_SQLS][256];
int g_nstatic_sqls = 0;

typedef struct esqlite_connection esqlite_connection;
typedef struct esqlite_backup esqlite_backup;
typedef struct esqlite_thread esqlite_thread;
typedef struct esqlite_control_data esqlite_control_data;

struct esqlite_control_data {
    char addresses[MAX_CONNECTIONS][255];
    int ports[MAX_CONNECTIONS];
    int types[MAX_CONNECTIONS];
    // connection prefixes
    ErlNifBinary prefixes[MAX_CONNECTIONS];
    char isopen[MAX_CONNECTIONS];
};

struct esqlite_thread {
    queue *commands;
    unsigned int dbcount;
    unsigned int inactivity;
    ErlNifTid tid;
    int alive;
    unsigned int index;
    // so currently executing connection data is accessible from wal callback
    esqlite_connection *curConn; 
    // MAX_CONNECTIONS (8) servers to replicate write log to
    int sockets[MAX_CONNECTIONS];
    int socket_types[MAX_CONNECTIONS];
    esqlite_control_data *control;

    // Prepared statements. 2d array (for every type list of sqls and versions)
    int prepSize;
    int prepVersions[MAX_PREP_SQLS][MAX_PREP_SQLS];
    char* prepSqls[MAX_PREP_SQLS][MAX_PREP_SQLS];
};
int g_nthreads;

esqlite_thread* g_threads;
esqlite_thread g_control_thread;

ErlNifUInt64 g_dbcount = 0;
ErlNifMutex *g_dbcount_mutex = NULL;


/* database connection context */
struct esqlite_connection{
    unsigned int thread;
    unsigned int randnum;
    sqlite3 *db;
    char open;
    int nPages;
    int nPrevPages;
    ErlNifUInt64 writeNumber;
    ErlNifUInt64 writeTermNumber;
    char wal_configured;
    // For every write:
    // over how many connections data has been sent
    char nSent;
    // Set bit for every failed attempt to write to socket of connection
    char failFlags;
    // 0   - do not replicate
    // > 0 - replicate to socket types that match number
    int doReplicate;
    // Fixed part of packet prefix
    ErlNifBinary packetPrefix;
    // Variable part of packet prefix
    ErlNifBinary packetVarPrefix;

    sqlite3_stmt *staticPrepared[MAX_STATIC_SQLS];
    sqlite3_stmt **prepared;
    int *prepVersions;
};

/* backup object */
struct esqlite_backup {
    sqlite3_backup *b;
    int pages_for_step;
    unsigned int thread;
    sqlite3 *dst;
    sqlite3 *src;
};


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
    cmd_stop,
    cmd_backup_init,
    cmd_backup_step,
    cmd_backup_finish,
    cmd_interrupt,
    cmd_tcp_connect,
    cmd_set_socket,
    cmd_tcp_reconnect,
    cmd_bind_insert,
    cmd_alltunnel_call,
    cmd_store_prepared
} command_type;

typedef struct {
    command_type type;

    ErlNifEnv *env;
    ERL_NIF_TERM ref; 
    ErlNifPid pid;
    ERL_NIF_TERM arg;
    ERL_NIF_TERM arg1;
    ERL_NIF_TERM arg2;
    ERL_NIF_TERM arg3;
    ERL_NIF_TERM arg4;
    sqlite3_stmt *stmt;
    esqlite_connection *conn;
    void *p;
} esqlite_command;

ERL_NIF_TERM atom_ok;
ERL_NIF_TERM atom_false;
ERL_NIF_TERM atom_error;
ERL_NIF_TERM atom_rows;
ERL_NIF_TERM atom_columns;
ERL_NIF_TERM atom_undefined;
ERL_NIF_TERM atom_rowid;
ERL_NIF_TERM atom_changes;
ERL_NIF_TERM atom_done;
static ERL_NIF_TERM make_cell(ErlNifEnv *env, sqlite3_stmt *statement, unsigned int i);
static ERL_NIF_TERM push_command(int thread, void *cmd);
static ERL_NIF_TERM make_binary(ErlNifEnv *env, const void *bytes, unsigned int size);
int wal_hook(void *data,sqlite3* db,const char* nm,int npages);
void write32bit(char *p, int v);
void write16bit(char *p, int v);
void wal_page_hook(void *data,void *page,int pagesize,void* header, int headersize);
void *command_create(int threadnum);
static ERL_NIF_TERM do_tcp_connect1(esqlite_command *cmd, esqlite_thread* thread, int pos);
static int bind_cell(ErlNifEnv *env, const ERL_NIF_TERM cell, sqlite3_stmt *stmt, unsigned int i);
void errLogCallback(void *pArg, int iErrCode, const char *zMsg);
void fail_send(int i);

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

// static ERL_NIF_TERM 
// make_row_tuple(ErlNifEnv *env, ERL_NIF_TERM value) 
// {
//     return enif_make_tuple2(env, make_atom(env, "row"), value);
// }


int 
wal_hook(void *data,sqlite3* db,const char* nm,int npages)
{
    esqlite_connection *conn = (esqlite_connection *) data;
    conn->nPrevPages = conn->nPages;
    conn->nPages = npages;
    return SQLITE_OK;
}

void 
write32bit(char *p, int v)
{
  p[0] = (char)(v>>24);
  p[1] = (char)(v>>16);
  p[2] = (char)(v>>8);
  p[3] = (char)v;
}
void 
write16bit(char *p, int v)
{
  p[0] = (char)(v>>8);
  p[1] = (char)v;
}


void 
wal_page_hook(void *data,void *page,int pagesize,void* header, int headersize)
{
    esqlite_thread *thread = (esqlite_thread *) data;
    esqlite_connection *conn = thread->curConn;
    int i = 0;
    int completeSize = 0;
#ifndef  _WIN32
    struct iovec iov[PACKET_ITEMS];
#else
    WSABUF iov[PACKET_ITEMS];
#endif
    char packetLen[4];
    char lenPrefix[2];
    char lenPage[2];
    char lenVarPrefix[2];
    char lenHeader = (char)headersize;
    char buff[PAGE_BUFF_SIZE];
    int buffUsed;
    int rt;
    // char confirm[7] = {0,0,0,0,0,0,0};

    if (!conn->doReplicate)
    {
        return;
    }
    conn->nSent = conn->failFlags = 0;


    if (pagesize > 0 && LZ4_COMPRESSBOUND(pagesize) < PAGE_BUFF_SIZE)
    {
        buffUsed = LZ4_compress((char*)page,(char*)(buff),pagesize);
    }
    else
    {
        buffUsed = 0;
    }
    completeSize = buffUsed+2+headersize+1+conn->packetPrefix.size+2+conn->packetVarPrefix.size+2;
    write16bit(lenPage,buffUsed);
    write32bit(packetLen,completeSize);
    write16bit(lenPrefix,conn->packetPrefix.size);
    write16bit(lenVarPrefix,conn->packetVarPrefix.size);

#ifndef _WIN32
    // Entire size
    iov[0].iov_base = packetLen;
    iov[0].iov_len = 4;
    // Prefix size and prefix data
    iov[1].iov_base = lenPrefix;
    iov[1].iov_len = 2;
    iov[2].iov_base = conn->packetPrefix.data;
    iov[2].iov_len = conn->packetPrefix.size;
    // Variable prefix
    iov[3].iov_base = lenVarPrefix;
    iov[3].iov_len = 2;
    iov[4].iov_base = conn->packetVarPrefix.data;
    iov[4].iov_len = conn->packetVarPrefix.size;
    // header size and header data
    iov[5].iov_base = &lenHeader;
    iov[5].iov_len = 1;
    iov[6].iov_base = header;
    iov[6].iov_len = headersize;
    // page size and page data
    iov[7].iov_base = lenPage;
    iov[7].iov_len = 2;
    iov[8].iov_base = buff;
    iov[8].iov_len = buffUsed;
#else
    // Entire size
    iov[0].buf = packetLen;
    iov[0].len = 4;
    // Prefix size and prefix data
    iov[1].buf = lenPrefix;
    iov[1].len = 2;
    iov[2].buf = conn->packetPrefix.data;
    iov[2].len = conn->packetPrefix.size;
    // Variable prefix
    iov[3].buf = lenVarPrefix;
    iov[3].len = 2;
    iov[4].buf = conn->packetVarPrefix.data;
    iov[4].len = conn->packetVarPrefix.size;
    // header size and header data
    iov[5].buf = &lenHeader;
    iov[5].len = 1;
    iov[6].buf = header;
    iov[6].len = headersize;
    // page size and page data
    iov[7].buf = lenPage;
    iov[7].len = 2;
    iov[8].buf = buff;
    iov[8].len = buffUsed;
#endif

    for (i = 0; i < MAX_CONNECTIONS; i++)
    {
        if (thread->sockets[i] > 3 && thread->socket_types[i] <= conn->doReplicate)
        {
            // sockets are blocking. We presume we are not
            //  network bound thus there should not be a lot of blocking
#ifndef _WIN32            
            rt = writev(thread->sockets[i],iov,PACKET_ITEMS);
#else
            if (WSASend(thread->sockets[i],iov,PACKET_ITEMS, &rt, 0, NULL, NULL) != 0)
                rt = 0;
#endif
            if (rt != completeSize+4)
            {
                conn->failFlags |= (1 << i);
                close(thread->sockets[i]);
                thread->sockets[i] = 0;
                fail_send(i);
            }
            else
            {
                // rt = recv(thread->sockets[i],confirm,6,0);
                // if (rt != 6 || confirm[4] != 'o' || confirm[5] != 'k')
                // {
                //     conn->failFlags |= (1 << i);
                //     close(thread->sockets[i]);
                //     thread->sockets[i] = 0;
                //     fail_send(i);
                // }
                conn->nSent++;
            }
        }
    }

    // var prefix only sent with first packet
    conn->packetVarPrefix.data = NULL;
    conn->packetVarPrefix.size = 0;
}

void fail_send(int i)
{
    esqlite_command *ncmd = NULL;
    // tell control thread to create new connections for position i
    void *item = command_create(-1);
    ncmd = queue_get_item_data(item);
    ncmd->type = cmd_tcp_connect;
    ncmd->arg3 = enif_make_int(ncmd->env,i);
    push_command(-1, item);
}


static ERL_NIF_TERM
do_all_tunnel_call(esqlite_command *cmd,esqlite_thread *thread)
{
#ifndef  _WIN32
    struct iovec iov[PACKET_ITEMS];
#else
    WSABUF iov[PACKET_ITEMS];
#endif
    char packetLen[4];
    char lenBin[2];
    ErlNifBinary bin;
    int nsent = 0, i = 0, rt = 0;
    // char confirm[7] = {0,0,0,0,0,0,0};

    enif_inspect_iolist_as_binary(cmd->env,cmd->arg,&(bin));

    write32bit(packetLen,bin.size);
    write16bit(lenBin,bin.size);

#ifndef  _WIN32
    iov[0].iov_base = packetLen;
    iov[0].iov_len = 4;
    iov[1].iov_len = bin.size;
    iov[1].iov_base = bin.data;
#else
    iov[0].buf = packetLen;
    iov[0].len = 4;
    iov[1].len = bin.size;
    iov[1].buf = bin.data;
#endif

    for (i = 0; i < MAX_CONNECTIONS; i++)
    {
        if (thread->sockets[i] > 3 && thread->socket_types[i] == 1)
        {
#ifndef _WIN32
            rt = writev(thread->sockets[i],iov,2);
#else
            if (WSASend(thread->sockets[i],iov,2, &rt, 0, NULL, NULL) != 0)
                rt = 0;
#endif
            if (rt == -1)
            {
                close(thread->sockets[i]);
                thread->sockets[i] = 0;

                fail_send(i);
            }
            // rt = recv(thread->sockets[i],confirm,6,0);
            // if (rt != 6 || confirm[4] != 'o' || confirm[5] != 'k')
            // {
            //     close(thread->sockets[i]);
            //     thread->sockets[i] = 0;

            //     fail_send(i);
            // }
            nsent++;
        }
    }

    return enif_make_int(cmd->env,nsent);
}


static ERL_NIF_TERM
do_store_prepared_table(esqlite_command *cmd,esqlite_thread *thread)
{
    // Delete old table of prepared statements and set new one.
    const ERL_NIF_TERM *versTuple;
    const ERL_NIF_TERM *sqlTuple;
    const ERL_NIF_TERM *versRow;
    const ERL_NIF_TERM *sqlRow;
    int tupleSize,rowSize,i,j;
    ErlNifBinary bin;

    memset(thread->prepVersions,0,sizeof(thread->prepVersions));
    for (i = 0; i < MAX_PREP_SQLS; i++)
    {
        for (j = 0; j < MAX_PREP_SQLS; j++)
        {
            free(thread->prepSqls[i][j]);
            thread->prepSqls[i][j] = NULL;
        }
    }

    // {{1,2,2,0,0,..},{0,0,0,0,...}}
    if (!enif_get_tuple(cmd->env, cmd->arg, &tupleSize, &versTuple))
        return false;
    // {{"select....","insert into..."},{"select....","update ...."}}
    if (!enif_get_tuple(cmd->env, cmd->arg1, &tupleSize, &sqlTuple))
        return false;

    if (tupleSize > MAX_PREP_SQLS)
        return atom_false;

    for (i = 0; i < tupleSize; i++)
    {
        if (!enif_get_tuple(cmd->env, versTuple[i], &rowSize, &versRow))
            break;
        if (!enif_get_tuple(cmd->env, sqlTuple[i], &rowSize, &sqlRow))
            break;

        thread->prepSize = rowSize;
        for (j = 0; j < rowSize; j++)
        {
            enif_get_int(cmd->env,versRow[j],&(thread->prepVersions[i][j]));
            if (enif_is_list(cmd->env,sqlRow[j]) || enif_is_binary(cmd->env,sqlRow[j]))
            {
                enif_inspect_iolist_as_binary(cmd->env,sqlRow[j],&bin);
                thread->prepSqls[i][j] = malloc(bin.size+1);
                thread->prepSqls[i][j][bin.size] = 0;
                memcpy(thread->prepSqls[i][j],bin.data,bin.size);
            }
        }
    }
    return atom_ok;
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
    // if(error_code == SQLITE_MISUSE) 
    //     return "Sqlite3 was invoked incorrectly.";

    return sqlite3_errmsg(db); 
}

static ERL_NIF_TERM
make_sqlite3_error_tuple(ErlNifEnv *env,const char* calledfrom, int error_code, sqlite3 *db) 
{
    const char *error_code_msg = get_sqlite3_return_code_msg(error_code);
    const char *msg = get_sqlite3_error_msg(error_code, db);
    
    if (calledfrom == NULL)
        return enif_make_tuple2(env, atom_error, 
            enif_make_tuple3(env, enif_make_string(env,"",ERL_NIF_LATIN1),
                              make_atom(env, error_code_msg), 
                              enif_make_string(env, msg, ERL_NIF_LATIN1)));
    else
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

void *
command_create(int threadnum)
{
    esqlite_thread *thread = NULL;
    void *item;
    esqlite_command *cmd;
    if (threadnum == -1)
        thread = &g_control_thread;
    else
        thread = &(g_threads[threadnum]);

    item = queue_get_item(thread->commands);
    cmd = queue_get_item_data(item);
    if (cmd == NULL)
    {
        cmd = (esqlite_command *) enif_alloc(sizeof(esqlite_command));
        cmd->env = enif_alloc_env();
        queue_set_item_data(item,cmd);
    }
    cmd->type = cmd_unknown;
    cmd->ref = 0;
    cmd->arg = cmd->arg1 = cmd->arg2 = cmd->arg3 = cmd->arg4 = 0;
    cmd->stmt = NULL;
    cmd->conn = NULL;
    cmd->p = NULL;

    return item;
}


/*
 *
 */
static void 
destruct_esqlite_connection(ErlNifEnv *env, void *arg)
{
    esqlite_connection *conn = (esqlite_connection *) arg;
    void *item;
    esqlite_command *cmd;
    int i = 0;

    if (!conn->packetPrefix.size)
        enif_release_binary(&conn->packetPrefix);

    if (!conn->open)
        return;
    conn->open = 0;

    if (conn->prepared != NULL)
    {
        for (i = 0; i < MAX_PREP_SQLS; i++)
        {
            if (conn->prepared[i] != 0)
            {
                sqlite3_finalize(conn->prepared[i]);
            }
                
            conn->prepared[i] = NULL;
        }
        free(conn->prepVersions);
        conn->prepVersions = NULL;
    }
    free(conn->prepared);
    conn->prepared = NULL;
    for (i = 0; i < MAX_STATIC_SQLS; i++)
    {
        sqlite3_finalize(conn->staticPrepared[i]);
        conn->staticPrepared[i] = NULL;
    }
    
    item = command_create(conn->thread);
    cmd = queue_get_item_data(item);


    cmd->type = cmd_close;
    cmd->p = conn->db;
    cmd->ref = 0;

    push_command(conn->thread, item);
}

static void
destruct_esqlite_backup(ErlNifEnv *env, void *arg)
{
    esqlite_backup *p = (esqlite_backup *)arg;
    if (p->b)
    {
        void *item = command_create(p->thread);
        esqlite_command *cmd = queue_get_item_data(item);
  
        cmd->type = cmd_backup_finish;
        cmd->p = p;
        cmd->ref = 0;

        push_command(p->thread, item);
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

    sqlite3_randomness(sizeof(cmd->conn->randnum), &(cmd->conn->randnum));

    /* Open the database. 
     */
    rc = sqlite3_open(filename,&(cmd->conn->db));

    if(rc != SQLITE_OK) {
        error = make_sqlite3_error_tuple(cmd->env, "sqlite3_open", rc, cmd->conn->db);
        sqlite3_close(cmd->conn->db);
        cmd->conn->db = NULL;
        return error;
    }

    cmd->conn->nPages = cmd->conn->nPrevPages = 0;
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

static ERL_NIF_TERM
do_backup_init(esqlite_command *cmd, esqlite_thread *thread) 
{
    esqlite_backup *p = (esqlite_backup *)cmd->p;
    ERL_NIF_TERM backup_term;

    p->b = sqlite3_backup_init(p->dst,"main",p->src,"main");
    backup_term = enif_make_resource(cmd->env, p);

    enif_release_resource(p);
    if (!(p->b))
        return atom_error;
    else
        return enif_make_tuple2(cmd->env,atom_ok,backup_term);
}

static ERL_NIF_TERM
do_backup_step(esqlite_command *cmd, esqlite_thread *thread) 
{
    esqlite_backup *p = (esqlite_backup *)cmd->p;
    int rt = sqlite3_backup_step(p->b,p->pages_for_step);
    enif_release_resource(p);

    switch (rt)
    {
        case SQLITE_OK:
            return atom_ok;
        case SQLITE_DONE:
            return atom_done;
        default:
            return atom_error;
    }
}

static ERL_NIF_TERM
do_backup_finish(esqlite_command *cmd, esqlite_thread *thread) 
{
    if (cmd->ref)
    {
        esqlite_backup *p = (esqlite_backup *)cmd->p;
        if (p->b)
        {
            sqlite3_backup_finish(p->b);
            p->b = NULL;
        }
        enif_release_resource(p);
    }
    else
    {
        sqlite3_backup *b = (sqlite3_backup *)cmd->p;
        sqlite3_backup_finish(b);
    }
    
    return atom_ok;
}

static ERL_NIF_TERM
do_interrupt(esqlite_command *cmd, esqlite_thread *thread) 
{
    sqlite3_interrupt(cmd->conn->db);
    enif_release_resource(cmd->conn);
    return atom_error;
}

static ERL_NIF_TERM
do_tcp_reconnect(esqlite_command *cmd, esqlite_thread *thread)
{
    int i;

    if (!thread->control)
        return atom_ok;

    for (i = 0; i < MAX_CONNECTIONS; i++)
    {
        // address set and not open
        if (thread->control->addresses[i][0] && !thread->control->isopen[i])
        {
            do_tcp_connect1(cmd,thread, i);
        }
    }
    return atom_ok;
}

static ERL_NIF_TERM
do_tcp_connect(esqlite_command *cmd, esqlite_thread *thread)
{
    int pos;
    ErlNifBinary bin;

    if (!thread->control)
    {
        thread->control = enif_alloc(sizeof(esqlite_control_data));
        memset(thread->control,0,sizeof(esqlite_control_data));
    }
    if (!enif_get_int(cmd->env,cmd->arg3,&pos))
        return enif_make_badarg(cmd->env);

    if (pos < 0 || pos > 7)
        return enif_make_badarg(cmd->env);

    // this can be called from erlang, or it can be called
    // from a thread that has lost connection.
    // If called from a thread, only pos is sent in arg3. Everything else
    //  has already been set on first call from erlang.
    if (cmd->arg)
    {
        if (!enif_get_string(cmd->env, cmd->arg,thread->control->addresses[pos],255,ERL_NIF_LATIN1))
            return enif_make_badarg(cmd->env);
        if (!enif_get_int(cmd->env,cmd->arg1,&(thread->control->ports[pos])))
            return enif_make_badarg(cmd->env);
        if (!enif_inspect_iolist_as_binary(cmd->env,cmd->arg2,&bin))
            return enif_make_badarg(cmd->env);

        enif_alloc_binary(bin.size,&(thread->control->prefixes[pos]));
        memcpy(thread->control->prefixes[pos].data,bin.data,bin.size);
        if (cmd->arg4)
        {
            if (!enif_get_int(cmd->env,cmd->arg4,&(thread->control->types[pos])))
                return enif_make_badarg(cmd->env);
        }
        else
            thread->control->types[pos] = 1;
    }
    else
    {
        bin = thread->control->prefixes[pos];
    }

    return do_tcp_connect1(cmd,thread,pos);
}

static ERL_NIF_TERM
do_tcp_connect1(esqlite_command *cmd, esqlite_thread* thread, int pos)
{
    int i;
    struct sockaddr_in addr;
    int fd;
    ERL_NIF_TERM result = atom_ok;
    esqlite_command *ncmd = NULL;
#ifndef _WIN32
    struct iovec iov[2];
#else
    WSABUF iov[2];
#endif
    char packetLen[4];
    int *sockets;
    char confirm[7] = {0,0,0,0,0,0,0};
    int flag = 1, rt = 0, error = 0, opts;
    socklen_t errlen = sizeof error;
    struct timeval timeout;
    fd_set fdset;


    write32bit(packetLen,thread->control->prefixes[pos].size);
#ifndef _WIN32
    iov[0].iov_base = packetLen;
    iov[0].iov_len = 4;
    iov[1].iov_base = thread->control->prefixes[pos].data;
    iov[1].iov_len = thread->control->prefixes[pos].size;
#else
    iov[0].buf = packetLen;
    iov[0].len = 4;
    iov[1].buf = thread->control->prefixes[pos].data;
    iov[1].len = thread->control->prefixes[pos].size;
#endif

    sockets = enif_alloc(sizeof(int)*g_nthreads);
    memset(sockets,0,sizeof(int)*g_nthreads);

    for (i = 0; i < g_nthreads; i++)
    {
        fd = socket(AF_INET,SOCK_STREAM,0);

#ifndef _WIN32
        if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1)
#else
        opts = 1;
        if (ioctlsocket(fd, FIONBIO, &opts) != 0)
#endif
        {
            close(fd);
            result = make_error_tuple(cmd->env,"unable to set nonblock");
            break;
        }

        memset(&addr,0,sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = inet_addr(thread->control->addresses[pos]);
        addr.sin_port = htons(thread->control->ports[pos]);
        rt = connect(fd, (const void *)&addr, sizeof(addr));
#ifndef _WIN32
        if (errno != EINPROGRESS)
#else
        if (WSAGetLastError() != WSAEWOULDBLOCK)
#endif
        {
            close(fd);
            result = make_error_tuple(cmd->env,"unable to connect");
            break;
        }

        FD_ZERO(&fdset);
        FD_SET(fd, &fdset);
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;

        rt = select(fd + 1, NULL, &fdset, NULL, &timeout);
#ifndef _WIN32
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &errlen);
#else
        getsockopt(fd, SOL_SOCKET, SO_ERROR, (char*)&error, &errlen);
#endif
        
        if (rt != 1 || error != 0)
        {
            close(fd);
            result = make_error_tuple(cmd->env,"unable to connect");
            break;
        }

#ifndef _WIN32
        opts = fcntl(fd,F_GETFL);
        if (fcntl(fd, F_SETFL, opts & (~O_NONBLOCK)) == -1 || fcntl(fd,F_GETFL) & O_NONBLOCK)
#else
        opts = 0;
        if (ioctlsocket(fd, FIONBIO, &opts) != 0)
#endif
        {
            close(fd);
            result = make_error_tuple(cmd->env,"unable to set blocking mode");
            break;
        }

        if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (char*)&flag, sizeof(int)) != 0)
        {
            close(fd);
            result = make_error_tuple(cmd->env,"unable to set sockopt");
            break;
        }
        if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char*)&flag, sizeof(int)) != 0)
        {
            close(fd);
            result = make_error_tuple(cmd->env,"unable to set sockopt");
            break;
        }
        if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(int)) != 0)
        {
            close(fd);
            result = make_error_tuple(cmd->env,"unable to set sockopt");
            break;
        }

        
        timeout.tv_sec = 2;
        timeout.tv_usec = 0;
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (char *)&timeout, sizeof(timeout));
        timeout.tv_sec = 2;
        timeout.tv_usec = 0;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (char *)&timeout,sizeof(timeout));
#ifndef _WIN32
        rt = writev(fd,iov,2);
#else
        if (WSASend(fd,iov,2, &rt, 0, NULL, NULL) != 0)
            rt = 0;
#endif
        if (thread->control->prefixes[pos].size+4 != rt)
        {
            close(fd);
            result = make_error_tuple(cmd->env,"unable to initialize");
            break;
        }

        rt = recv(fd,confirm,6,0);
        if (rt != 6 || confirm[4] != 'o' || confirm[5] != 'k')
        {
            close(fd);
            result = make_error_tuple(cmd->env,"unable to initialize");
            break;
        }

        sockets[i] = fd;
    }

    if (result == atom_ok)
    {
        thread->control->isopen[pos] = 1;

        for (i = 0; i < g_nthreads; i++)
        {
            void *item = command_create(i);
            ncmd = queue_get_item_data(item);
            ncmd->type = cmd_set_socket;
            ncmd->arg = enif_make_int(ncmd->env,sockets[i]);
            ncmd->arg1 = enif_make_int(ncmd->env,pos);
            ncmd->arg2 = enif_make_int(ncmd->env,thread->control->types[pos]);
            push_command(i, item);
        }
    }
    else
    {
        thread->control->isopen[pos] = 0;

        for (i = 0; i < g_nthreads; i++)
        {
            if (sockets[i])
                close(sockets[i]);
        }
    }
    enif_free(sockets);

    return result;
}

/* 
 */
// static ERL_NIF_TERM
// do_exec(esqlite_command *cmd, esqlite_thread *thread)
// {
//     ErlNifBinary bin;
//     int rc;

//     enif_inspect_iolist_as_binary(cmd->env, cmd->arg, &bin);

//     rc = sqlite3_exec(cmd->conn->db, (char *)bin.data, NULL, NULL, NULL);
//     if(rc != SQLITE_OK)
// 	    return make_sqlite3_error_tuple(cmd->env,"sqlite3_exec in do_exec", rc, cmd->conn->db);
//     enif_release_resource(cmd->conn);

//     return atom_ok;
// }

static ERL_NIF_TERM
do_bind_insert(esqlite_command *cmd, esqlite_thread *thread)
{
    ERL_NIF_TERM list, rowhead, rowlist;
    ErlNifBinary bin;
    sqlite3_stmt *statement = NULL;
    int rc = 0;
    ERL_NIF_TERM result;
    int i = 0,insertCounter = 0;

    list = cmd->arg1;

    if (!enif_inspect_iolist_as_binary(cmd->env, cmd->arg, &bin))
        return make_error_tuple(cmd->env, "not iolist");

    rc = sqlite3_prepare_v2(cmd->conn->db, (char *)(bin.data), bin.size, &(statement), NULL);
    if(rc != SQLITE_OK)
    {
        sqlite3_finalize(statement);
        return make_sqlite3_error_tuple(cmd->env, NULL, rc, cmd->conn->db);
    }

    for (insertCounter = 0; enif_get_list_cell(cmd->env, list, &rowlist, &list); insertCounter++)
    {
        for (i = 0; enif_get_list_cell(cmd->env, rowlist, &rowhead, &rowlist); i++)
        {
            if (bind_cell(cmd->env, rowhead, statement, i+1) == -1)
            {
                list = enif_make_list(cmd->env,0);
                break;
            }
        }
        rc = sqlite3_step(statement);
        if (rc != SQLITE_DONE)
        {
            break;
        }
        sqlite3_reset(statement);
    }
    sqlite3_finalize(statement);
    enif_release_resource(cmd->conn);

    if (rc == SQLITE_DONE)
    {
        result = enif_make_list1(cmd->env, enif_make_tuple3(cmd->env,atom_changes,
                                             enif_make_int64(cmd->env,sqlite3_last_insert_rowid(cmd->conn->db)), 
                                            enif_make_int(cmd->env,sqlite3_changes(cmd->conn->db))));
        return enif_make_tuple2(cmd->env,atom_ok,result);
    }
    else
        return atom_false;
}

/* 
 */
static ERL_NIF_TERM
do_exec_script(esqlite_command *cmd, esqlite_thread *thread)
{
    ErlNifBinary bin;
    int rc = 0,i;
    unsigned int rowcount = 0;
    sqlite3_stmt *statement = NULL;
    const char *readpoint;
    const char *end;
    int column_count;
    ERL_NIF_TERM *array;
    ERL_NIF_TERM column_names;
    ERL_NIF_TERM results;
    // insert records are a list of lists.
    ERL_NIF_TERM listTop = 0, headTop = 0, headBot = 0;
    int skip = 0;
    int statementlen = 0;
    ERL_NIF_TERM rows;
    char *errat = NULL;
    char dofinalize = 1;
    
    const ERL_NIF_TERM *insertRow;
    int rowLen = 0;
    listTop = cmd->arg4;


    if (!cmd->conn->wal_configured)
        cmd->conn->wal_configured = SQLITE_OK == 
                                    sqlite3_wal_page_hook(cmd->conn->db,
                                                wal_page_hook,
                                                thread,
                                                (unsigned long long int*)&(cmd->conn->writeNumber),
                                                (unsigned long long int*)&(cmd->conn->writeTermNumber));

    if (!enif_inspect_iolist_as_binary(cmd->env, cmd->arg, &bin))
        return make_error_tuple(cmd->env, "not iolist");

    if (cmd->arg1)
    {
        enif_get_uint64(cmd->env,cmd->arg1,&(cmd->conn->writeTermNumber));
        enif_get_uint64(cmd->env,cmd->arg2,&(cmd->conn->writeNumber));
        enif_inspect_binary(cmd->env,cmd->arg3,&(cmd->conn->packetVarPrefix));
    }

    end = (char*)bin.data + bin.size;
    readpoint = (char*)bin.data;
    results = enif_make_list(cmd->env,0);
    

    while (readpoint < end)
    {
        if (readpoint[0] == '$')
            skip = 1;
        else
            skip = 0;
        statementlen = end-readpoint;
        
        // if _insert, then this is a prepared statement with multiple rows in arg4
        if (statementlen >= 8 && cmd->arg4 && readpoint[skip] == '_' && (readpoint[skip+1] == 'i' || readpoint[skip+1] == 'I') &&
             (readpoint[skip+2] == 'n' || readpoint[skip+2] == 'N'))
        {
            skip++;
            rc = sqlite3_prepare_v2(cmd->conn->db, (char *)(readpoint+skip), statementlen, &(statement), &readpoint);
            if(rc != SQLITE_OK)
            {
                errat = "_prepare";
                sqlite3_finalize(statement);
                break;
            }
            rc = SQLITE_DONE;

            if (!enif_get_list_cell(cmd->env, listTop, &headTop, &listTop))
            {
                rc = SQLITE_INTERRUPT;
                sqlite3_finalize(statement);
                break;
            }

            // Move over a list of records.
            // First element is ignored as it is presumed to be record name.
            while (rc == SQLITE_DONE && enif_get_list_cell(cmd->env, headTop, &headBot, &headTop))
            {
                if (!enif_get_tuple(cmd->env, headBot, &rowLen, &insertRow) && rowLen > 1 && rowLen < 100)
                {
                    rc = SQLITE_INTERRUPT;
                    break;
                }

                for (i = 1; i < rowLen; i++)
                {
                    if (bind_cell(cmd->env, insertRow[i], statement, i) == -1)
                    {
                        errat = "cant_bind";
                        sqlite3_finalize(statement);
                        break;                        
                    }
                }
                rc = sqlite3_step(statement);
                sqlite3_reset(statement);
            }
        }
        else
        {
            if (statementlen >= 5 && readpoint[skip] == '#' &&  (readpoint[skip+4] == ';' || readpoint[skip+6] == ';'))
            {
                dofinalize = 0;                

                // static prepared statements
                if (readpoint[skip+1] == 's' || readpoint[skip+1] == 'd')
                {
                    i = (readpoint[skip+2] - '0')*10 + (readpoint[skip+3] - '0');
                    if (readpoint[skip+1] == 's')
                        skip = 1;

                    if (cmd->conn->staticPrepared[i] == NULL)
                    {
                        rc = sqlite3_prepare_v2(cmd->conn->db, (char *)g_static_sqls[i], -1, &(cmd->conn->staticPrepared[i]), NULL);
                        if(rc != SQLITE_OK)
                        {
                            errat = "prepare";
                            break;
                        }
                    }
                    statement = cmd->conn->staticPrepared[i];
                }
                // user set prepared statements
                else if ((readpoint[skip+1] == 'r' || readpoint[skip+1] == 'w') && readpoint[skip+6] == ';')
                {
                    // actor type index
                    i = (readpoint[skip+2] - '0')*10 + (readpoint[skip+3] - '0');
                    // statement index
                    rowLen = (readpoint[skip+4] - '0')*10 + (readpoint[skip+5] - '0');

                    if (thread->prepSize <= i)
                    {
                        errat = "prepare";
                        break;
                    }

                    if (cmd->conn->prepared == NULL)
                    {
                        cmd->conn->prepared = malloc(MAX_PREP_SQLS*sizeof(sqlite3_stmt*));
                        memset(cmd->conn->prepared,0,MAX_PREP_SQLS*sizeof(sqlite3_stmt*));
                        cmd->conn->prepVersions = malloc(MAX_PREP_SQLS*sizeof(int));
                        memset(cmd->conn->prepVersions,0,MAX_PREP_SQLS*sizeof(int));
                    }

                    if (cmd->conn->prepared[rowLen] == NULL || cmd->conn->prepVersions[rowLen] != thread->prepVersions[i][rowLen])
                    {
                        if (thread->prepSqls[i][rowLen] == NULL)
                        {
                            errat = "prepare";
                            break;
                        }
                        if (cmd->conn->prepared[rowLen] != NULL)
                            sqlite3_finalize(cmd->conn->prepared[rowLen]);

                        rc = sqlite3_prepare_v2(cmd->conn->db, thread->prepSqls[i][rowLen], -1, &(cmd->conn->prepared[rowLen]), NULL);
                        if(rc != SQLITE_OK)
                        {
                            errat = "prepare";
                            break;
                        }
                        cmd->conn->prepVersions[rowLen] = thread->prepVersions[i][rowLen];
                    }

                    statement = cmd->conn->prepared[rowLen];
                }
                else
                {
                    errat = "prepare";
                    break;
                }
                
                if (sqlite3_bind_parameter_count(statement))
                {
                    // Single prepared statement can have multiple rows that it wants to execute
                    // We execute one at a time. If headTop /= 0, we are still in previous statement.
                    if (headTop == 0)
                    {
                        // List can be:
                        // [[Tuple1,Tuple2],...] -> for every statement, list of rows.
                        // [[[Column1,Column2,..],..],...] -> for every statement, for every row, list of columns.
                        if (!enif_get_list_cell(cmd->env, listTop, &headTop, &listTop))
                        {
                            rc = SQLITE_INTERRUPT;
                            break;
                        }
                    }

                    // Every row is a list. It can be a list of tuples (which implies records thus we start at offset 1),
                    //   or a list of columns.
                    if (enif_get_list_cell(cmd->env, headTop, &headBot, &headTop))
                    {
                        // If tuple bind from tuple
                        if (enif_is_tuple(cmd->env, headBot))
                        {
                            if (!enif_get_tuple(cmd->env, headBot, &rowLen, &insertRow) && rowLen > 1 && rowLen < 100)
                            {
                                rc = SQLITE_INTERRUPT;
                                break;
                            }

                            for (i = 1; i < rowLen; i++)
                            {
                                if (bind_cell(cmd->env, insertRow[i], statement, i) == -1)
                                {
                                    errat = "cant_bind";
                                    rc = SQLITE_INTERRUPT;
                                    break;                        
                                }
                            }
                        }
                        // If list, bind from list
                        else if (enif_is_list(cmd->env,headBot))
                        {
                            ERL_NIF_TERM rowHead;
                            // Index is from 1 because sqlite bind param start with 1 not 0
                            for (i = 1; enif_get_list_cell(cmd->env, headBot, &rowHead, &headBot); i++)
                            {
                                if (bind_cell(cmd->env, rowHead, statement, i) == -1)
                                {
                                    errat = "cant_bind";
                                    rc = SQLITE_INTERRUPT;
                                    break;
                                }
                            }
                        }
                        if (rc == SQLITE_INTERRUPT)
                        {
                            break;
                        }
                    }
                    // We reached end of list of rows. Move on to next statement
                    else
                    {
                        headTop = 0;
                        if (readpoint[skip+1] == 'r' || readpoint[skip+1] == 'w')
                            readpoint += 7;
                        else
                            readpoint += 5;
                        continue;
                    }
                }
                else
                    readpoint += 5;
            }
            else
            {
                dofinalize = 1;
                rc = sqlite3_prepare_v2(cmd->conn->db, (char *)(readpoint+skip), statementlen, &(statement), &readpoint);
                if(rc != SQLITE_OK)
                {
                    errat = "prepare";
                    sqlite3_finalize(statement);
                    break;
                }
            }
             
            column_count = sqlite3_column_count(statement);
            if (column_count > 0)
            {
                array = (ERL_NIF_TERM *)malloc(sizeof(ERL_NIF_TERM) * column_count);

                for(i = 0; i < column_count; i++) 
                {
                    const char* cname = sqlite3_column_name(statement, i);
                    array[i] = make_binary(cmd->env, cname,strlen(cname));
                }

                column_names = enif_make_tuple_from_array(cmd->env, array, column_count);
                free(array);
            }
            
                    
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
        }


        if (rc == SQLITE_ERROR || rc == SQLITE_INTERRUPT)
        {
            errat = "step";
            dofinalize ? sqlite3_finalize(statement) : sqlite3_reset(statement);
            break;
        }
        
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
        }
        dofinalize ? sqlite3_finalize(statement) : sqlite3_reset(statement);
        statement = NULL;
    }


    // has number of pages changed
    // if (cmd->conn->nPages != nPages)
    // {
    //     if (rc != SQLITE_ERROR && rc != SQLITE_INTERRUPT)
    //     {
    //         write32bit(pagesBuff,cmd->conn->nPages - cmd->conn->nPrevPages);
    //         // wal_page_hook(thread,NULL,0,pagesBuff,4);
    //     }
    //     // else
    //     //     wal_page_hook(thread,NULL,0,NULL,0);
    // }

    enif_release_resource(cmd->conn);
    // Errors are from 1 to 99.
    if (rc > 0 && rc < 100 && rc != SQLITE_INTERRUPT)
        return make_sqlite3_error_tuple(cmd->env, errat, rc, cmd->conn->db);
    else if (rc == SQLITE_INTERRUPT)
    {
        return make_error_tuple(cmd->env, "query_aborted");
    }
    else
    {
        return make_ok_tuple(cmd->env,results);
    }
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
    {
        return sqlite3_bind_int(stmt, i, the_int);
    }
        

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
    {
        return sqlite3_bind_text(stmt, i, (char *) the_blob.data, the_blob.size, SQLITE_TRANSIENT);
    }
        

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
do_close(esqlite_command *cmd,esqlite_thread *thread)
{
    ERL_NIF_TERM ret;
    int rc;

    sqlite3 *db = (sqlite3 *)cmd->p;
    rc = sqlite3_close(db);

    if(rc != SQLITE_OK)
    {
        ret = make_error_tuple(cmd->env,"sqlite3_close in do_close");
    }
    else
    {
        if (cmd->conn != NULL)
        {
            cmd->conn->open = 0;
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
            cmd->arg1 = 0;
            return do_exec_script(cmd,thread);
        }
    }
    case cmd_exec_script:
        return do_exec_script(cmd,thread);
    case cmd_store_prepared:
        return do_store_prepared_table(cmd,thread);
    case cmd_close:
	    return do_close(cmd,thread);
    case cmd_backup_init:
        return do_backup_init(cmd,thread);
    case cmd_backup_finish:
        return do_backup_finish(cmd,thread);
    case cmd_backup_step:
        return do_backup_step(cmd,thread);
    case cmd_interrupt:
        return do_interrupt(cmd,thread);
    case cmd_unknown:
        return atom_ok;
    case cmd_tcp_connect:
        return do_tcp_connect(cmd,thread);
    case cmd_tcp_reconnect:
        return do_tcp_reconnect(cmd,thread);
    case cmd_bind_insert:
        return do_bind_insert(cmd,thread);
    case cmd_alltunnel_call:
        return do_all_tunnel_call(cmd,thread);
    case cmd_set_socket:
    {
        int fd = 0;
        int pos = -1;
        int type = 1;
        if (!enif_get_int(cmd->env,cmd->arg,&fd))
            return atom_error;
        if (!enif_get_int(cmd->env,cmd->arg1,&pos))
            return atom_error;
        if (!enif_get_int(cmd->env,cmd->arg2,&type))
            return atom_error;
        
        if (fd > 3 && pos >= 0 && pos < 8)
        {
            if (thread->sockets[pos] > 3)
            {
                char zero[4];
                memset(zero,0,4);
                
                // check if connection open, if it is do not use new socket
                if (write(thread->sockets[pos],zero,4) == -1)
                {
                    close(thread->sockets[pos]);
                    thread->sockets[pos] = fd;
                }
                else
                {
                    close(fd);
                }
            }
            else
            {
                thread->sockets[pos] = fd;
                thread->socket_types[pos] = type;
            }
            return atom_ok;
        }
        return atom_error;
    }
    default:
	    return make_error_tuple(cmd->env, "invalid_command");
    }
}

// esqlite_connection *conn
static ERL_NIF_TERM
push_command(int threadnum, void *item) 
{
    esqlite_thread *thread = NULL;
    if (threadnum == -1)
        thread = &g_control_thread;
    else
        thread = &(g_threads[threadnum]);

    if(!queue_push(thread->commands, item)) 
    {
        esqlite_command *cmd = queue_get_item_data(item);
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
    int i,j;
    esqlite_thread* data = (esqlite_thread*)arg;
    esqlite_command *cmd;
    data->alive = 1;

    while(1) 
    {
        void *item = queue_pop(data->commands);
        cmd = queue_get_item_data(item);
        data->curConn = cmd->conn;

        if (cmd->type == cmd_stop)
        {
            queue_recycle(data->commands,item);
            break;
        }
        else
        {
            if (cmd->ref == 0)
            {
                evaluate_command(cmd,data);
                enif_clear_env(cmd->env);
            }
            else
            {
                enif_send(NULL, &cmd->pid, cmd->env, make_answer(cmd, evaluate_command(cmd,data)));
                enif_clear_env(cmd->env);
            }
            queue_recycle(data->commands,item);
        }
    }
    queue_destroy(data->commands);

    if (data->control)
    {
        enif_free(data->control);
        data->control = NULL;
    }
  
    for (i = 0; i < MAX_PREP_SQLS; i++)
    {
        for (j = 0; j < MAX_PREP_SQLS; j++)
        {
            free(data->prepSqls[i][j]);
            data->prepSqls[i][j] = NULL;
        }
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

    enif_consume_timeslice(env,500);

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
    void *item;
     
    if(!(argc == 4 || argc == 5)) 
	    return enif_make_badarg(env);     
    if(!enif_is_ref(env, argv[0])) 
	    return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[1], &pid)) 
	    return make_error_tuple(env, "invalid_pid");

    conn = enif_alloc_resource(esqlite_connection_type, sizeof(esqlite_connection));
    if(!conn) 
        return make_error_tuple(env, "no_memory");
    if(!enif_get_uint(env, argv[3], &(conn->thread))) 
        return make_error_tuple(env, "invalid_thread_number");
    
    memset(conn,0,sizeof(esqlite_connection));
    conn->thread %= g_nthreads;

    item = command_create(conn->thread);
    cmd = queue_get_item_data(item);
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
    enif_consume_timeslice(env,500);
    return enif_make_tuple2(env,push_command(conn->thread, item),db_conn);
}

static ERL_NIF_TERM
esqlite_replicate_opts(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection* conn;
    ErlNifBinary bin;

    if (!(argc == 2 || argc == 3))
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &conn))
    {
        return enif_make_badarg(env);
    }

    if (argc == 3)
    {
        if (!enif_get_int(env,argv[2],&(conn->doReplicate)))
            return enif_make_badarg(env);
    }
    else
        conn->doReplicate = 1;

    if (!conn->packetPrefix.size)
        enif_release_binary(&conn->packetPrefix);

    if (!enif_inspect_iolist_as_binary(env, argv[1], &bin))
        return enif_make_badarg(env);

    enif_alloc_binary(bin.size,&(conn->packetPrefix));
    memcpy(conn->packetPrefix.data,bin.data,bin.size);

    enif_consume_timeslice(env,500);
    return atom_ok;
}

static ERL_NIF_TERM
esqlite_replicate_status(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection* conn;

    if (argc != 1)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &conn))
    {
        return enif_make_badarg(env);
    }
    
    enif_consume_timeslice(env,500);
    return enif_make_tuple2(env,enif_make_int(env,conn->nSent),enif_make_int(env,conn->failFlags));
}
// Called with: ref,pid, ip, port, connect string, connection number
static ERL_NIF_TERM
tcp_connect(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_command *cmd = NULL;
    ErlNifPid pid;
    void *item;

    if (!(argc == 6 || argc == 7))
        return enif_make_badarg(env);

    if(!enif_is_ref(env, argv[0])) 
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[1], &pid)) 
        return make_error_tuple(env, "invalid_pid");
    if (!enif_is_list(env,argv[2]))
        return enif_make_badarg(env);
    if (!enif_is_number(env,argv[3]))
        return enif_make_badarg(env);
    if (!(enif_is_binary(env,argv[4]) || enif_is_list(env,argv[2])))
        return enif_make_badarg(env);
    if (!enif_is_number(env,argv[5]))
        return enif_make_badarg(env);
    if (argc == 7 && !enif_is_number(env,argv[6]))
        return enif_make_badarg(env);

    item = command_create(-1);
    cmd = queue_get_item_data(item);
    if(!cmd)
        return make_error_tuple(env, "command_create_failed");
    cmd->type = cmd_tcp_connect;
    cmd->arg = enif_make_copy(cmd->env,argv[2]);
    cmd->arg1 = enif_make_copy(cmd->env,argv[3]);
    cmd->arg2 = enif_make_copy(cmd->env,argv[4]);
    cmd->arg3 = enif_make_copy(cmd->env,argv[5]);
    if (argc == 7)
        cmd->arg4 = enif_make_copy(cmd->env,argv[6]);
    cmd->ref = enif_make_copy(cmd->env, argv[0]);
    cmd->pid = pid;

    enif_consume_timeslice(env,500);
    return push_command(-1,item);
}

static ERL_NIF_TERM
tcp_reconnect(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_command *cmd = NULL;

    void *item = command_create(-1);
    cmd = queue_get_item_data(item);
    if(!cmd)
        return make_error_tuple(env, "command_create_failed");
    cmd->type = cmd_tcp_reconnect;

    enif_consume_timeslice(env,500);

    return push_command(-1,item);
}

static ERL_NIF_TERM
wal_header(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    int szPage;
    unsigned int aCksum[2];                /* Checksum for wal-header */
    ErlNifBinary binOut;

    if (argc != 1)
        enif_make_badarg(env);

    if (!enif_get_int(env,argv[0],&szPage))
        return enif_make_badarg(env);

    enif_alloc_binary(WAL_HDRSIZE,&binOut);

    sqlite3Put4byte(&binOut.data[0], (WAL_MAGIC | SQLITE_BIGENDIAN));
    sqlite3Put4byte(&binOut.data[4], WAL_MAX_VERSION);
    sqlite3Put4byte(&binOut.data[8], szPage);
    sqlite3Put4byte(&binOut.data[12], 0);
    // Salt has fixed values
    sqlite3Put4byte(&binOut.data[16], 123456789);
    sqlite3Put4byte(&binOut.data[20], 987654321);
    // memcpy(&binOut.data[16], binSalt.data, 8);
    walChecksumBytes(1, binOut.data, WAL_HDRSIZE-2*4, 0, aCksum);
    sqlite3Put4byte(&binOut.data[24], aCksum[0]);
    sqlite3Put4byte(&binOut.data[28], aCksum[1]);

    enif_consume_timeslice(env,500);
    return enif_make_binary(env,&binOut);
}

static ERL_NIF_TERM
wal_checksum(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    unsigned int cksum1,cksum2;
    unsigned int aCksum[2] = {0,0};
    ErlNifBinary bin;
    int size;

    if (argc != 4)
        enif_make_badarg(env);

    if (!enif_inspect_binary(env,argv[0], &bin))
        return enif_make_badarg(env);

    if (!enif_get_uint(env,argv[1], &cksum1))
        return enif_make_badarg(env);
    if (!enif_get_uint(env,argv[2], &cksum2))
        return enif_make_badarg(env); 
    if (!enif_get_int(env,argv[3], &size))
        return enif_make_badarg(env); 

    aCksum[0] = cksum1;
    aCksum[1] = cksum2;

    walChecksumBytes(1, bin.data, size, aCksum, aCksum);

    enif_consume_timeslice(env,500);

    return enif_make_tuple2(env,enif_make_uint(env,aCksum[0]),enif_make_uint(env,aCksum[1]));
}

static ERL_NIF_TERM
esqlite_interrupt_query(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_command *cmd = NULL;
    esqlite_connection* conn;
    void *item;

    if(argc != 1) 
        return enif_make_badarg(env);  

    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &conn))
    {
        return enif_make_badarg(env);
    }   
    item = command_create(-1);
    cmd = queue_get_item_data(item);
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");
    cmd->type = cmd_interrupt;
    cmd->ref = 0;
    cmd->conn = conn;
    enif_keep_resource(conn);

    enif_consume_timeslice(env,500);

    return push_command(-1, item);
}


static ERL_NIF_TERM
esqlite_backup_init(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *dbsrc;
    esqlite_connection *dbdest;
    esqlite_command *cmd = NULL;
    esqlite_backup* backup;
    ErlNifPid pid;
    void *item;
     
    if(argc != 4) 
        return enif_make_badarg(env);  
    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &dbdest))
    {
        return enif_make_badarg(env);
    }
    if(!enif_get_resource(env, argv[1], esqlite_connection_type, (void **) &dbsrc))
    {
        return enif_make_badarg(env);
    }   

    if(!enif_is_ref(env, argv[2])) 
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[3], &pid)) 
        return make_error_tuple(env, "invalid_pid"); 

    item = command_create(dbsrc->thread);
    cmd = queue_get_item_data(item);
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");
    
    backup = enif_alloc_resource(esqlite_backup_type,sizeof(esqlite_backup));
    backup->b = NULL;
    backup->thread = dbsrc->thread;
    backup->dst = dbdest->db;
    backup->src = dbsrc->db;

    /* command */
    cmd->type = cmd_backup_init;
    cmd->ref = enif_make_copy(cmd->env, argv[2]);
    cmd->pid = pid;
    cmd->arg = enif_make_copy(cmd->env, argv[3]);
    cmd->p = backup;

    enif_consume_timeslice(env,500);

    return push_command(backup->thread, item);
}

static ERL_NIF_TERM
esqlite_backup_finish(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_command *cmd = NULL;
    esqlite_backup* backup;
    ErlNifPid pid;
    void *item;

    if (argc != 3)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite_backup_type, (void **) &backup))
    {
        return enif_make_badarg(env);
    }
    if(!enif_is_ref(env, argv[1])) 
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[2], &pid)) 
        return make_error_tuple(env, "invalid_pid"); 

    item = command_create(backup->thread);
    cmd = queue_get_item_data(item);
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_backup_finish;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->arg = enif_make_copy(cmd->env, argv[2]);
    cmd->p = backup;

    enif_keep_resource(backup);

    enif_consume_timeslice(env,500);

    return push_command(backup->thread, item);
}

static ERL_NIF_TERM
esqlite_backup_pages(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_backup* backup;

    if (argc != 1)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite_backup_type, (void **) &backup))
    {
        return enif_make_badarg(env);
    }

    enif_consume_timeslice(env,500);
    return enif_make_tuple2(env, enif_make_int(env,sqlite3_backup_pagecount(backup->b)),
                                 enif_make_int(env,sqlite3_backup_remaining(backup->b)));
}

static ERL_NIF_TERM
esqlite_backup_step(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_command *cmd = NULL;
    esqlite_backup* backup;
    ErlNifPid pid;
    void *item;

    if (argc != 4)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite_backup_type, (void **) &backup))
    {
        return enif_make_badarg(env);
    }
    if(!enif_is_ref(env, argv[1])) 
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[2], &pid)) 
        return make_error_tuple(env, "invalid_pid"); 
    if(!enif_get_int(env, argv[3], &(backup->pages_for_step))) 
        return make_error_tuple(env, "invalid_thread_number");

    item = command_create(backup->thread);
    cmd = queue_get_item_data(item);
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_backup_step;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->arg = enif_make_copy(cmd->env, argv[2]);
    cmd->p = backup;

    enif_keep_resource(backup);

    enif_consume_timeslice(env,500);

    return push_command(backup->thread, item);
}

static ERL_NIF_TERM
esqlite_lz4_compress(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary binIn;
    ErlNifBinary binOut;
    int size;
    ERL_NIF_TERM termbin;

    if (argc != 1)
        return enif_make_badarg(env);

    if (!enif_inspect_iolist_as_binary(env, argv[0], &binIn))
        return enif_make_badarg(env);

    enif_alloc_binary(LZ4_COMPRESSBOUND(binIn.size),&binOut);

    size = LZ4_compress((char*)binIn.data,(char*)binOut.data,binIn.size);
    termbin = enif_make_binary(env,&binOut);
    enif_release_binary(&binOut);

    enif_consume_timeslice(env,500);
    return enif_make_tuple2(env,termbin,enif_make_int(env,size));
}

static ERL_NIF_TERM
esqlite_lz4_decompress(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary binIn;
    ErlNifBinary binOut;
    int sizeOriginal;
    int sizeReadNum;
    int rt;

    if (argc != 2 && argc != 3)
        return enif_make_badarg(env);

    if (!enif_inspect_iolist_as_binary(env, argv[0], &binIn))
        return enif_make_badarg(env);

    if (!enif_get_int(env,argv[1],&sizeOriginal))
        return enif_make_badarg(env);

    if (argc == 3)
    {
        if (!enif_get_int(env,argv[2],&sizeReadNum))
            return enif_make_badarg(env);
    }
    else
        sizeReadNum = binIn.size;

    enif_alloc_binary(sizeOriginal,&binOut);
    rt = LZ4_decompress_safe((char*)binIn.data,(char*)binOut.data,sizeReadNum,sizeOriginal);
    enif_consume_timeslice(env,500);
    if (rt > 0)
    {
        ERL_NIF_TERM termout = enif_make_binary(env,&binOut);
        enif_release_binary(&binOut);
        return termout;
    }
    else
    {
        enif_release_binary(&binOut);
        return atom_error;
    }
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
    return enif_make_tuple2(env, enif_make_int(env,db->nPrevPages),
                                 enif_make_int(env,db->nPages)
                                 // enif_make_int(env,db->socketFlag)
                                 );
}


static ERL_NIF_TERM
all_tunnel_call(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_command *cmd = NULL;
    ErlNifPid pid;
    int i;
    void *item;

    if (argc != 3)
        return enif_make_badarg(env);

    if(!enif_is_ref(env, argv[0])) 
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[1], &pid)) 
        return make_error_tuple(env, "invalid_pid");
    if (!(enif_is_binary(env,argv[2]) || enif_is_list(env,argv[2])))
        return make_error_tuple(env, "invalid bin");

    for (i = 0; i < g_nthreads; i++)
    {
        item = command_create(i);
        cmd = queue_get_item_data(item);
        if(!cmd) 
            return make_error_tuple(env, "command_create_failed");

        /* command */
        cmd->type = cmd_alltunnel_call;
        cmd->ref = enif_make_copy(cmd->env, argv[0]);
        cmd->pid = pid;
        cmd->arg = enif_make_copy(cmd->env, argv[2]);

        enif_consume_timeslice(env,500);

        push_command(i, item);
    }
    return atom_ok;
}


static ERL_NIF_TERM
store_prepared_table(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_command *cmd = NULL;
    int i;
    void *item;

    if (argc != 2)
        return enif_make_badarg(env);

    if (!(enif_is_tuple(env,argv[0]) && enif_is_tuple(env,argv[1])))
        return enif_make_badarg(env);

    for (i = 0; i < g_nthreads; i++)
    {
        item = command_create(i);
        cmd = queue_get_item_data(item);
        if(!cmd) 
            return make_error_tuple(env, "command_create_failed");

        /* command */
        cmd->type = cmd_store_prepared;
        cmd->arg = enif_make_copy(cmd->env, argv[0]);
        cmd->arg1 = enif_make_copy(cmd->env, argv[1]);

        enif_consume_timeslice(env,500);

        push_command(i, item);
    }
    return atom_ok;
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
    void *item;
     
    if(argc != 7 && argc != 8) 
        return enif_make_badarg(env);  
    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &db))
        return enif_make_badarg(env);
        
    if(!enif_is_ref(env, argv[1])) 
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[2], &pid)) 
        return make_error_tuple(env, "invalid_pid"); 
    if (!(enif_is_binary(env,argv[3]) || enif_is_list(env,argv[3])))
        return make_error_tuple(env,"invalid sql");
    if (!enif_is_number(env,argv[4]))
        return make_error_tuple(env, "term not number"); 
    if (!enif_is_number(env,argv[5]))
        return make_error_tuple(env, "index not number"); 
    if (!enif_is_binary(env,argv[6]))
        return make_error_tuple(env, "appendparam not binary");    

    
    item = command_create(db->thread);
    cmd = queue_get_item_data(item);
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");
     
    /* command */
    cmd->type = cmd_exec_script;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->arg = enif_make_copy(cmd->env, argv[3]);  // sql string
    cmd->arg1 = enif_make_copy(cmd->env, argv[4]); // term
    cmd->arg2 = enif_make_copy(cmd->env, argv[5]); // index
    cmd->arg3 = enif_make_copy(cmd->env, argv[6]); // appendentries param binary
    if (argc == 8)
        cmd->arg4 = enif_make_copy(cmd->env, argv[7]);  // records for bulk insert
    cmd->conn = db;
    enif_keep_resource(db);

    enif_consume_timeslice(env,500);

    return push_command(db->thread, item);
}


static ERL_NIF_TERM 
esqlite_bind_insert(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *db;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;
    void *item;

    if(argc != 5) 
        return enif_make_badarg(env);  

    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &db))
        return enif_make_badarg(env);
        
    if(!enif_is_ref(env, argv[1])) 
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[2], &pid)) 
        return make_error_tuple(env, "invalid_pid"); 
    if (!(enif_is_binary(env,argv[3]) || enif_is_list(env,argv[3])))
        return make_error_tuple(env,"invalid sql");
    if (!(enif_is_list(env,argv[4])))
        return make_error_tuple(env,"invalid bind parameters");

    item = command_create(db->thread);
    cmd = queue_get_item_data(item);
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");
     
    /* command */
    cmd->type = cmd_bind_insert;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->arg = enif_make_copy(cmd->env, argv[3]);  // sql string
    cmd->arg1 = enif_make_copy(cmd->env, argv[4]); // bind list
    cmd->conn = db;
    enif_keep_resource(db);

    enif_consume_timeslice(env,500);

    return push_command(db->thread, item);
}


static ERL_NIF_TERM 
esqlite_noop(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection *conn;
    esqlite_command *cmd = NULL;
    ErlNifPid pid;
    void *item;

    if(argc != 3) 
        return enif_make_badarg(env);
    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &conn))
        return enif_make_badarg(env);
    if(!enif_is_ref(env, argv[1])) 
        return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[2], &pid)) 
        return make_error_tuple(env, "invalid_pid"); 

    item = command_create(conn->thread);
    cmd = queue_get_item_data(item);
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_unknown;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;

    return push_command(conn->thread, item);
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
    void *item;

    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &conn))
	    return enif_make_badarg(env);
    if(!enif_is_ref(env, argv[1])) 
	    return make_error_tuple(env, "invalid_ref");
    if(!enif_get_local_pid(env, argv[2], &pid)) 
	    return make_error_tuple(env, "invalid_pid"); 

    item = command_create(conn->thread);
    cmd = queue_get_item_data(item);
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_close;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->conn = conn;
    cmd->p = conn->db;
    enif_keep_resource(conn);

    enif_consume_timeslice(env,500);
    return push_command(conn->thread, item);

    // return atom_ok;
}

void errLogCallback(void *pArg, int iErrCode, const char *zMsg)
{
}


/*
 * Load the nif. Initialize some stuff and such
 */
static int 
on_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{
    ErlNifResourceType *rt;
    int i = 0;
    const ERL_NIF_TERM *param;

#ifdef _WIN32
    WSADATA wsd;
    if (WSAStartup(MAKEWORD(1, 1), &wsd) != 0)
        return -1;
#endif

    sqlite3_initialize();
    sqlite3_config(SQLITE_CONFIG_LOG, errLogCallback, NULL);

    // sqlite3_vfs_register(sqlite3_vfs_find("unix-nolock"), 1);    

    atom_false = enif_make_atom(env,"false");
    atom_ok = enif_make_atom(env,"ok");
    atom_rows = enif_make_atom(env,"rows");
    atom_columns = enif_make_atom(env,"columns");
    atom_error = enif_make_atom(env,"error");
    atom_undefined = enif_make_atom(env,"undefined");
    atom_rowid = enif_make_atom(env,"rowid");
    atom_changes = enif_make_atom(env,"changes");
    atom_done = enif_make_atom(env,"done");
    
    if (enif_is_tuple(env,info))
    {
        enif_get_tuple(env,info,&i,&param);
        if (i != 2)
            return -1;

        if (!enif_get_int(env,param[0],&g_nthreads))
            return -1;

        if (!enif_is_tuple(env,param[1]))
            return -1;

        enif_get_tuple(env,param[1],&i,&param);
        if (i > MAX_STATIC_SQLS)
            return -1;

        g_nstatic_sqls = i;
        for (i = 0; i < g_nstatic_sqls; i++)
            enif_get_string(env,param[i],g_static_sqls[i],256,ERL_NIF_LATIN1);
    }
    else if (!enif_get_int(env,info,&g_nthreads))
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

    rt =  enif_open_resource_type(env, "esqlite3_nif", "esqlite_backup_type",
                   destruct_esqlite_backup, ERL_NIF_RT_CREATE, NULL);
    if(!rt) 
        return -1;
    esqlite_backup_type = rt;
    
    g_threads = (esqlite_thread*)malloc(sizeof(esqlite_thread)*g_nthreads);
    memset(g_threads,0,sizeof(esqlite_thread)*g_nthreads);
    memset(&g_control_thread,0,sizeof(esqlite_thread));
    g_control_thread.index = -1;
    g_control_thread.commands = queue_create(command_destroy);

    if(enif_thread_create("esqlite_connection", &(g_control_thread.tid), esqlite_thread_func, &(g_control_thread), NULL) != 0) 
    {
        printf("Unable to create esqlite3 thread\r\n");
        return -1;
    }
    
    for (i = 0; i < g_nthreads; i++)
    {
        g_threads[i].index = i;
        g_threads[i].commands = queue_create(command_destroy);

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
    for (i = -1; i < g_nthreads; i++)
    {
        void *item = command_create(i);
        esqlite_command *cmd = queue_get_item_data(item);
        cmd->type = cmd_stop;
        push_command(i, item);

        if (i >= 0)
            enif_thread_join((ErlNifTid)g_threads[i].tid,NULL);
        else
            enif_thread_join((ErlNifTid)g_control_thread.tid,NULL);
    }
}

static ErlNifFunc nif_funcs[] = {
    {"open", 4, esqlite_open},
    {"open", 5, esqlite_open},
    {"replicate_opts",2,esqlite_replicate_opts},
    {"replicate_opts",3,esqlite_replicate_opts},
    {"replicate_status",1,esqlite_replicate_status},
    // {"exec", 4, esqlite_exec},
    {"exec_script", 7, esqlite_exec_script},
    {"exec_script", 8, esqlite_exec_script},
    {"bind_insert",5,esqlite_bind_insert},
    // {"prepare", 4, esqlite_prepare},
    // {"step", 3, esqlite_step},
    {"noop", 3, esqlite_noop},
    // {"bind", 4, esqlite_bind},
    {"parse_helper",2,parse_helper},
    // {"column_names", 3, esqlite_column_names},
    {"close", 3, esqlite_close},
    {"wal_pages",1,esqlite_wal_pages},
    {"backup_init",4,esqlite_backup_init},
    {"backup_finish",3,esqlite_backup_finish},
    {"backup_step",4,esqlite_backup_step},
    {"backup_pages",1,esqlite_backup_pages},
    {"interrupt_query",1,esqlite_interrupt_query},
    {"lz4_compress",1,esqlite_lz4_compress},
    {"lz4_decompress",2,esqlite_lz4_decompress},
    {"lz4_decompress",3,esqlite_lz4_decompress},
    {"tcp_connect",6,tcp_connect},
    {"tcp_connect",7,tcp_connect},
    {"tcp_reconnect",0,tcp_reconnect},
    {"wal_header",1,wal_header},
    {"wal_checksum",4,wal_checksum},
    {"all_tunnel_call",3,all_tunnel_call},
    {"store_prepared_table",2,store_prepared_table}
};

ERL_NIF_INIT(esqlite3_nif, nif_funcs, on_load, NULL, NULL, on_unload);
