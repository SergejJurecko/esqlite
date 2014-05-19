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

#ifdef linux
#define _GNU_SOURCE 1
#include <sys/mman.h>
#include <dlfcn.h>
#endif
#include <unistd.h>

#include <erl_nif.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>
#ifndef  _WIN32
#include <unistd.h>
#endif
#include <fcntl.h>

#include "lz4.h"
#include "queue.h"

// #ifdef __APPLE__
#ifndef _WIN32
// Directly include sqlite3.c
// This way we are sure the included version of sqlite3 is actually used.
// If we were to just include "sqlite3.h" OSX would actually use /usr/lib/libsqlite3.dylib
#define SQLITE_API static 
#define SQLITE_EXTERN static 
#include "sqlite3.c" 
#else
#include "sqlite3.h"
#endif

#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/uio.h>
#include <netinet/tcp.h>

#define MAX_ATOM_LENGTH 255 /* from atom.h, not exposed in erlang include */
#define MAX_PATHNAME 512 /* unfortunately not in sqlite.h. */
#define PAGE_BUFF_SIZE 9000
#define MAX_CONNECTIONS 8
#define PACKET_ITEMS 9

static ErlNifResourceType *esqlite_connection_type = NULL;
static ErlNifResourceType *esqlite_statement_type = NULL;
static ErlNifResourceType *esqlite_backup_type = NULL;

typedef struct esqlite_connection esqlite_connection;
typedef struct esqlite_statement esqlite_statement;
typedef struct esqlite_backup esqlite_backup;
typedef struct esqlite_thread esqlite_thread;
typedef struct esqlite_control_data esqlite_control_data;

struct esqlite_control_data {
    char addresses[MAX_CONNECTIONS][255];
    int ports[MAX_CONNECTIONS];
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
    esqlite_control_data *control;
};
int g_nthreads;

esqlite_thread* g_threads;
esqlite_thread g_control_thread;

ErlNifUInt64 g_dbcount = 0;
ErlNifMutex *g_dbcount_mutex = NULL;

/* database connection context */
struct esqlite_connection{
    unsigned int thread;
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
    char doReplicate;
    // only first byte used. Out of 8 possible servers, bit is set for every 
    // server where data should be replicated to.
    // int socketFlag;
    // Fixed part of packet prefix
    ErlNifBinary packetPrefix;
    // Variable part of packet prefix
    ErlNifBinary packetVarPrefix;
};

/* prepared statement */
struct esqlite_statement {
    unsigned int thread;
    sqlite3_stmt *statement;
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
    cmd_tcp_reconnect
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
    esqlite_command *ncmd = NULL;
    int i = 0;
    int completeSize = 0;
    struct iovec iov[PACKET_ITEMS];
    char packetLen[4];
    char lenPrefix[2];
    char lenPage[2];
    char lenVarPrefix[2];
    char lenHeader = (char)headersize;
    char buff[PAGE_BUFF_SIZE];
    int buffUsed;
    int rt;

    if (!conn->doReplicate)
        return;
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

    for (i = 0; i < MAX_CONNECTIONS; i++)
    {
        if (thread->sockets[i] > 3)
        {
            // sockets are blocking. We presume we are not
            //  network bound thus there should not be a lot of blocking
            rt = writev(thread->sockets[i],iov,PACKET_ITEMS);
            if (rt == -1)
            {
                conn->failFlags |= (1 << i);

                close(thread->sockets[i]);
                thread->sockets[i] = 0;

                // tell control thread to create new connections for position i
                void *item = command_create(-1);
                ncmd = queue_get_item_data(item);
                ncmd->type = cmd_tcp_connect;
                ncmd->arg3 = enif_make_int(ncmd->env,i);
                push_command(-1, item);
            }
            else
                conn->nSent++;
        }
    }

    // var prefix only sent with first packet
    conn->packetVarPrefix.data = NULL;
    conn->packetVarPrefix.size = 0;
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

void *
command_create(int threadnum)
{
    esqlite_thread *thread = NULL;
    if (threadnum == -1)
        thread = &g_control_thread;
    else
        thread = &(g_threads[threadnum]);

    void *item = queue_get_item(thread->commands);
    esqlite_command *cmd = queue_get_item_data(item);
    if (cmd == NULL)
    {
        cmd = (esqlite_command *) enif_alloc(sizeof(esqlite_command));
        cmd->env = enif_alloc_env();
        queue_set_item_data(item,cmd);
    }
    cmd->type = cmd_unknown;
    cmd->ref = 0;
    cmd->arg = cmd->arg1 = cmd->arg2 = cmd->arg3 = 0;
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
    if (conn->open)
    {
        void *item = command_create(conn->thread);
        esqlite_command *cmd = queue_get_item_data(item);
  
        /* Send the stop command 
         */
        cmd->type = cmd_close;
        cmd->p = conn->db;
        cmd->ref = 0;

        push_command(conn->thread, item);
    }
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
    rc = sqlite3_open(filename,&(cmd->conn->db));

    if(rc != SQLITE_OK) {
        error = make_sqlite3_error_tuple(cmd->env, "sqlite3_open", rc, cmd->conn->db);
        sqlite3_close(cmd->conn->db);
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

    p->b = sqlite3_backup_init(p->dst,"main",p->src,"main");
    ERL_NIF_TERM backup_term = enif_make_resource(cmd->env, p);

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
        if (!thread->control->addresses[i][0] && !thread->control->isopen)
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
    struct iovec iov[2];
    char packetLen[4];
    int *sockets;


    write32bit(packetLen,thread->control->prefixes[pos].size);
    iov[0].iov_base = packetLen;
    iov[0].iov_len = 4;
    iov[1].iov_base = thread->control->prefixes[pos].data;
    iov[1].iov_len = thread->control->prefixes[pos].size;

    sockets = enif_alloc(sizeof(int)*g_nthreads);
    memset(sockets,0,sizeof(int)*g_nthreads);

    for (i = 0; i < g_nthreads; i++)
    {
        fd = socket(AF_INET,SOCK_STREAM,0);
        memset(&addr,0,sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = inet_addr(thread->control->addresses[pos]);
        addr.sin_port = htons(thread->control->ports[pos]);

        if (connect(fd, (const void *)&addr, sizeof(addr)) == -1)
        {
            close(fd);
            result = make_error_tuple(cmd->env,"unable to connect");
            break;
        }

        struct timeval timeout;      
        timeout.tv_sec = 2;
        timeout.tv_usec = 0;
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (char *)&timeout, sizeof(timeout));

        if (thread->control->prefixes[pos].size+4 != writev(fd,iov,2))
        {
            close(fd);
            result = make_error_tuple(cmd->env,"unable to initialize");
            break;
        }

        int flag = 1;
        setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (char*)&flag, sizeof(int));
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char*)&flag, sizeof(int));
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char*)&flag, sizeof(int));

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
    char pagesBuff[4];
    int nPages = cmd->conn->nPages;

    if (!cmd->conn->wal_configured)
        cmd->conn->wal_configured = SQLITE_OK == 
                                    sqlite3_wal_page_hook(cmd->conn->db,
                                                wal_page_hook,
                                                thread,
                                                (u64*)&(cmd->conn->writeNumber),
                                                (u64*)&(cmd->conn->writeTermNumber));

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

        if (rc == SQLITE_ERROR || rc == SQLITE_INTERRUPT)
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
        }
        sqlite3_finalize(statement);
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
    if (rc == SQLITE_ERROR)
        return make_sqlite3_error_tuple(cmd->env, "exec_script", rc, cmd->conn->db);
    else if (rc == SQLITE_INTERRUPT)
    {
        return make_error_tuple(cmd->env, "query_aborted");
    }
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
    
    sqlite3 *db = (sqlite3 *)cmd->p;
    rc = sqlite3_close(db);

    if(rc != SQLITE_OK)
    {
        ret = make_sqlite3_error_tuple(cmd->env,"sqlite3_close in do_close",rc,db);
    }
    else
    {
        if (cmd->conn != NULL)
        {
            if (!cmd->conn->packetPrefix.size)
                enif_release_binary(&cmd->conn->packetPrefix);
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
            cmd->arg1 = 0;
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
    case cmd_set_socket:
    {
        int fd = 0;
        int pos = -1;
        if (!enif_get_int(cmd->env,cmd->arg,&fd))
            return atom_error;
        if (!enif_get_int(cmd->env,cmd->arg1,&pos))
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
                thread->sockets[pos] = fd;
            
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
    if(!conn) 
        return make_error_tuple(env, "no_memory");
    if(!enif_get_uint(env, argv[3], &(conn->thread))) 
        return make_error_tuple(env, "invalid_thread_number");
    
    memset(conn,0,sizeof(esqlite_connection));
    conn->thread %= g_nthreads;

    void *item = command_create(conn->thread);
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
    return enif_make_tuple2(env,push_command(conn->thread, item),db_conn);
}

static ERL_NIF_TERM
esqlite_replicate_opts(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_connection* conn;
    ErlNifBinary bin;

    if (argc != 2)
        return enif_make_badarg(env);

    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &conn))
    {
        return enif_make_badarg(env);
    }

    conn->doReplicate = 1;

    // if (!enif_get_int(env,argv[1],&(conn->socketFlag)))
    //     return enif_make_badarg(env);
    if (!conn->packetPrefix.size)
        enif_release_binary(&conn->packetPrefix);

    if (!enif_inspect_iolist_as_binary(env, argv[1], &bin))
        return enif_make_badarg(env);

    enif_alloc_binary(bin.size,&(conn->packetPrefix));
    memcpy(conn->packetPrefix.data,bin.data,bin.size);

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
    
    return enif_make_tuple2(env,enif_make_int(env,conn->nSent),enif_make_int(env,conn->failFlags));
}
// Called with: ref,pid, ip, port, connect string, connection number
static ERL_NIF_TERM
tcp_connect(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_command *cmd = NULL;
    ErlNifPid pid;

    if (argc != 6)
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

    void *item = command_create(-1);
    cmd = queue_get_item_data(item);
    if(!cmd)
        return make_error_tuple(env, "command_create_failed");
    cmd->type = cmd_tcp_connect;
    cmd->arg = argv[2];
    cmd->arg1 = argv[3];
    cmd->arg2 = argv[4];
    cmd->arg3 = argv[5];
    cmd->ref = enif_make_copy(cmd->env, argv[0]);
    cmd->pid = pid;

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

    return push_command(-1,item);
}

static ERL_NIF_TERM
wal_header(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    int szPage;
    u32 aCksum[2];                /* Checksum for wal-header */
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

    return enif_make_binary(env,&binOut);
}

static ERL_NIF_TERM
esqlite_interrupt_query(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_command *cmd = NULL;
    esqlite_connection* conn;

    if(argc != 1) 
        return enif_make_badarg(env);  

    if(!enif_get_resource(env, argv[0], esqlite_connection_type, (void **) &conn))
    {
        return enif_make_badarg(env);
    }   
    void *item = command_create(-1);
    cmd = queue_get_item_data(item);
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");
    cmd->type = cmd_interrupt;
    cmd->ref = 0;
    cmd->conn = conn;
    enif_keep_resource(conn);

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

    void *item = command_create(dbsrc->thread);
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

    return push_command(backup->thread, item);
}

static ERL_NIF_TERM
esqlite_backup_finish(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_command *cmd = NULL;
    esqlite_backup* backup;
    ErlNifPid pid;

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

    void *item = command_create(backup->thread);
    cmd = queue_get_item_data(item);
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_backup_finish;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->arg = enif_make_copy(cmd->env, argv[2]);
    cmd->p = backup;

    enif_keep_resource(backup);

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

    return enif_make_tuple2(env, enif_make_int(env,sqlite3_backup_pagecount(backup->b)),
                                 enif_make_int(env,sqlite3_backup_remaining(backup->b)));
}

static ERL_NIF_TERM
esqlite_backup_step(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    esqlite_command *cmd = NULL;
    esqlite_backup* backup;
    ErlNifPid pid;

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

    void *item = command_create(backup->thread);
    cmd = queue_get_item_data(item);
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_backup_step;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->arg = enif_make_copy(cmd->env, argv[2]);
    cmd->p = backup;

    enif_keep_resource(backup);

    return push_command(backup->thread, item);
}

static ERL_NIF_TERM
esqlite_lz4_compress(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary binIn;
    ErlNifBinary binOut;

    if (argc != 1)
        return enif_make_badarg(env);

    if (!enif_inspect_iolist_as_binary(env, argv[0], &binIn))
        return enif_make_badarg(env);

    enif_alloc_binary(LZ4_COMPRESSBOUND(binIn.size),&binOut);

    int size = LZ4_compress((char*)binIn.data,(char*)binOut.data,binIn.size);
    ERL_NIF_TERM termbin = enif_make_binary(env,&binOut);
    enif_release_binary(&binOut);
    return enif_make_tuple2(env,termbin,enif_make_int(env,size));
}

static ERL_NIF_TERM
esqlite_lz4_decompress(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary binIn;
    ErlNifBinary binOut;
    int sizeOriginal;
    int sizeReadNum;

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
    int rt = LZ4_decompress_safe((char*)binIn.data,(char*)binOut.data,sizeReadNum,sizeOriginal);
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

    void *item = command_create(db->thread);
    cmd = queue_get_item_data(item);
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");
    
    /* command */
    cmd->type = cmd_exec;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->arg = enif_make_copy(cmd->env, argv[3]);
    cmd->conn = db;
    enif_keep_resource(db);

    return push_command(db->thread, item);
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
     
    if(argc != 7) 
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

    
    void *item = command_create(db->thread);
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
    cmd->conn = db;
    enif_keep_resource(db);

    return push_command(db->thread, item);
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

    void *item = command_create(conn->thread);
    cmd = queue_get_item_data(item);
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_prepare;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->arg = enif_make_copy(cmd->env, argv[3]);
    cmd->conn = conn;
    enif_keep_resource(conn);

    return push_command(conn->thread, item);
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

    void *item = command_create(stmt->thread);
    cmd = queue_get_item_data(item);
    if(!cmd) 
	    return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_bind;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->stmt = stmt->statement;
    cmd->arg = enif_make_copy(cmd->env, argv[3]);

    return push_command(stmt->thread, item);
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

    void *item = command_create(stmt->thread);
    cmd = queue_get_item_data(item);
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_step;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->stmt = stmt->statement;

    return push_command(stmt->thread, item);
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

    void *item = command_create(conn->thread);
    cmd = queue_get_item_data(item);
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_unknown;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;

    return push_command(conn->thread, item);
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

    void *item = command_create(stmt->thread);
    cmd = queue_get_item_data(item);
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_column_names;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->stmt = stmt->statement;

    return push_command(stmt->thread, item);
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

    void *item = command_create(conn->thread);
    cmd = queue_get_item_data(item);
    if(!cmd) 
        return make_error_tuple(env, "command_create_failed");

    cmd->type = cmd_close;
    cmd->ref = enif_make_copy(cmd->env, argv[1]);
    cmd->pid = pid;
    cmd->conn = conn;
    cmd->p = conn->db;
    enif_keep_resource(conn);

    return push_command(conn->thread, item);
}

/*
 * Load the nif. Initialize some stuff and such
 */
static int 
on_load(ErlNifEnv* env, void** priv, ERL_NIF_TERM info)
{
    ErlNifResourceType *rt;
    int i = 0;


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
    {"replicate_status",1,esqlite_replicate_status},
    {"exec", 4, esqlite_exec},
    {"exec_script", 7, esqlite_exec_script},
    {"prepare", 4, esqlite_prepare},
    {"step", 3, esqlite_step},
    {"noop", 3, esqlite_noop},
    // TODO: {"esqlite_bind", 3, esqlite_bind_named},
    {"bind", 4, esqlite_bind},
    {"parse_helper",2,parse_helper},
    {"column_names", 3, esqlite_column_names},
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
    {"tcp_reconnect",0,tcp_reconnect},
    {"wal_header",1,wal_header}
};

ERL_NIF_INIT(esqlite3_nif, nif_funcs, on_load, NULL, NULL, on_unload);
