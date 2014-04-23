%% @author Maas-Maarten Zeeman <mmzeeman@xs4all.nl>
%% @copyright 2011, 2012, 2013 Maas-Maarten Zeeman

%% @doc Low level erlang API for sqlite3 databases

%% Copyright 2011, 2012, 2013 Maas-Maarten Zeeman
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% 
%%     http://www.apache.org/licenses/LICENSE-2.0
%% 
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(esqlite3_nif).
-author("Maas-Maarten Zeeman <mmzeeman@xs4all.nl>").

%% low-level exports
-export([init/1,
         open/4, 
         open/5,
         exec/4, 
         exec_script/7, 
         prepare/4,
         step/3,
         finalize/3,
         bind/4,
         column_names/3,
         close/3,
         noop/3,
         parse_helper/2,
         wal_pages/1,
         backup_init/4,
         backup_finish/3,
         backup_step/4,
         backup_pages/1,
         interrupt_query/1,
         lz4_compress/1,
         lz4_decompress/2,
         lz4_decompress/3,
         tcp_connect/6,
         tcp_reconnect/0,
         replicate_opts/2,
         wal_header/2,
         replicate_status/1
]).

replicate_status(_) ->
    exit(nif_library_not_loaded).

wal_header(_,_) ->
    exit(nif_library_not_loaded).

noop(_,_,_) ->
    exit(nif_library_not_loaded).

replicate_opts(_,_) ->
    exit(nif_library_not_loaded).

tcp_connect(_,_,_,_,_,_) ->
    exit(nif_library_not_loaded).

tcp_reconnect() ->
    exit(nif_library_not_loaded).
    
interrupt_query(_) ->
    exit(nif_library_not_loaded).

parse_helper(_,_) ->
    exit(nif_library_not_loaded).

lz4_compress(_) ->
    exit(nif_library_not_loaded).

lz4_decompress(_,_) ->
    exit(nif_library_not_loaded).

lz4_decompress(_,_,_) ->
    exit(nif_library_not_loaded).

backup_init(_,_,_,_) ->
    exit(nif_library_not_loaded).

backup_finish(_,_,_) ->
    exit(nif_library_not_loaded).

backup_step(_,_,_,_) ->
    exit(nif_library_not_loaded).

backup_pages(_) ->
    exit(nif_library_not_loaded).

init(Threads) when is_integer(Threads) ->
    NifName = "esqlite3_nif",
    NifFileName = case code:priv_dir(esqlite) of
        {error, bad_name} -> filename:join("priv", NifName);
        Dir -> filename:join(Dir, NifName)
    end,
    case erlang:load_nif(NifFileName, Threads) of
        ok ->
            ok;
        {error,{upgrade,_}} ->
            ok;
        {error,{reload,_}} ->
            ok
    end.

wal_pages(_) ->
    exit(nif_library_not_loaded).

%% @doc Start a low level thread which will can handle sqlite3 calls. 
%%
%% @spec start() -> {ok, connection()} | {error, msg()}
% start() ->
%     exit(nif_library_not_loaded).

%% @doc Open the specified sqlite3 database.
%% 
%% Sends an asynchronous open command over the connection and returns
%% ok immediately. When the database is opened 
%%
%%  @spec open(connection(), reference(), pid(), string()) -> ok | {error, message()}
open(_Ref, _Dest, _Filename,_ThreadNumber) ->
    exit(nif_library_not_loaded).
open(_Ref, _Dest, _Filename,_ThreadNumber,_Sql) ->
    exit(nif_library_not_loaded).

%% @doc Exec the query.
%% 
%% Sends an asynchronous exec command over the connection and returns
%% ok immediately.
%%
%% When the statement is executed Dest will receive message {Ref, answer()}
%% with answer() integer | {error, reason()}
%%
%%  @spec exec(connection(), Ref::reference(), Dest::pid(), string()) -> ok | {error, message()}
exec(_Db, _Ref, _Dest, _Sql) ->
    exit(nif_library_not_loaded).

%% @doc Exec the query.
%% 
%% Sends an asynchronous exec command over the connection and returns
%% ok immediately.
%%
%% When the statement is executed Dest will receive message {Ref, answer()}
%% with answer() {changes,Rowid,NumChanges} | {Columns,Rows} | ok | {error, reason()}
%%
%%  @spec exec(connection(), Ref::reference(), Dest::pid(), string(),integer(),integer(),binary()) -> ok | {error, message()}
exec_script(_Db, _Ref, _Dest, _Sql,_Term,_Index,_AParam) ->
    exit(nif_library_not_loaded).

%% @doc
%%
%% @spec prepare(connection(), reference(), pid(), string()) -> ok | {error, message()}
prepare(_Db, _Ref, _Dest, _Sql) ->
    exit(nif_library_not_loaded).

%% @doc
%%
%% @spec step(statement(), reference(), pid()) -> ok | {error, message()}
step(_Stmt, _Ref, _Dest) ->
    exit(nif_library_not_loaded).

%% @doc
%%
%%
finalize(_Stmt, _Ref, _Dest) ->
    exit(nif_library_not_loaded).

%% @doc Bind parameters to a prepared statement. 
%%
%% @spec bind(statement(), reference(), pid(), []) -> ok | {error, message()} 
bind(_Stmt, _Ref, _Dest, _Args) ->
    exit(nif_library_not_loaded).

%% @doc Retrieve the column names of the prepared statement
%%
%% @spec column_names(statement(), reference(), pid()) -> {ok, tuple()} | {error, message()} 
column_names(_Stmt, _Ref, _Dest) ->
    exit(nif_library_not_loaded).

%% @doc Close the connection.
%%
%% @spec close(connection(), reference(), pid()) -> ok | {error, message()}
close(_Db, _Ref, _Dest) ->
    exit(nif_library_not_loaded).

    



