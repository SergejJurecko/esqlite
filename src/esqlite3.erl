%% @author Maas-Maarten Zeeman <mmzeeman@xs4all.nl>
%% @copyright 2011, 2012, 2013 Maas-Maarten Zeeman

%% @doc Erlang API for sqlite3 databases

%% Copyright 2011, 2012 Maas-Maarten Zeeman
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

-module(esqlite3).
-author("Maas-Maarten Zeeman <mmzeeman@xs4all.nl>").

%% higher-level export
-export([init/1,noop/1,
         open/1,open/2,open/3,
         exec_script/2,exec_script/3,exec_script/6,exec_script/4,exec_script/7,
         fetchone/1,
         fetchall/1,
         store_prepared_table/2,
         close/1,
         parse_helper/1,parse_helper/2,wal_pages/1,
         backup_init/2,backup_step/2,backup_finish/1,backup_pages/1,
         lz4_compress/1,lz4_decompress/2,lz4_decompress/3,
         replicate_opts/2,replicate_opts/3,replicate_status/1,tcp_connect/4,all_tunnel_call/1,
         tcp_connect_async/4,tcp_connect_async/5,make_wal_header/1,tcp_reconnect/0,wal_checksum/4,bind_insert/3]).


-type connection() :: tuple().
-type statement() :: term().
% -type sql() :: iolist().

store_prepared_table(Indexes,Sqls) when is_tuple(Indexes), is_tuple(Sqls), tuple_size(Indexes) == tuple_size(Sqls) ->
    esqlite3_nif:store_prepared_table(Indexes,Sqls).

make_wal_header(PageSize) ->
    esqlite3_nif:wal_header(PageSize).

wal_checksum(Bin,C1,C2,Size) ->
    esqlite3_nif:wal_checksum(Bin,C1,C2,Size).

parse_helper(Bin) ->
    parse_helper(Bin,0).
parse_helper(Bin,Offset) ->
    esqlite3_nif:parse_helper(Bin,Offset).

replicate_opts({connection, _Ref, Connection},PacketPrefix) ->
    esqlite3_nif:replicate_opts(Connection,PacketPrefix,1).
replicate_opts({connection, _Ref, Connection},PacketPrefix,Type) ->
    esqlite3_nif:replicate_opts(Connection,PacketPrefix,Type).

replicate_status({connection, _Ref, Connection}) ->
    esqlite3_nif:replicate_status(Connection).

tcp_connect(Ip,Port,ConnectStr,ConnNumber) ->
    Ref = make_ref(),
    esqlite3_nif:tcp_connect(Ref,self(),Ip,Port,ConnectStr,ConnNumber),
    receive_answer(Ref).
tcp_connect_async(Ip,Port,ConnectStr,ConnNumber) ->
    Ref = make_ref(),
    esqlite3_nif:tcp_connect(Ref,self(),Ip,Port,ConnectStr,ConnNumber),
    Ref.
tcp_connect_async(Ip,Port,ConnectStr,ConnNumber,Type) ->
    Ref = make_ref(),
    esqlite3_nif:tcp_connect(Ref,self(),Ip,Port,ConnectStr,ConnNumber,Type),
    Ref.
tcp_reconnect() ->
    esqlite3_nif:tcp_reconnect().

all_tunnel_call(Bin) ->
    Ref = make_ref(),
    ok = esqlite3_nif:all_tunnel_call(Ref,self(),Bin),
    receive_answer(Ref).

lz4_compress(B) ->
    esqlite3_nif:lz4_compress(B).
lz4_decompress(B,SizeOrig) ->
    esqlite3_nif:lz4_decompress(B,SizeOrig).
lz4_decompress(B,SizeOrig,SizeIn) ->
    esqlite3_nif:lz4_decompress(B,SizeOrig,SizeIn).

%% @doc Opens a sqlite3 database mentioned in Filename on first thread.
%%
-spec open(FileName) -> {ok, connection()} | {error, _} when
    FileName :: string().
open(Filename) ->
    open(Filename,0).

%% @doc Opens a sqlite3 database mentioned in Filename on thread ThreadNumber (valid ranges 0 - (InitThreads-1)).
%%
-spec open(FileName,ThreadNumber) -> {ok, connection()} | {error, _} when
    FileName :: string(), ThreadNumber :: integer().
open(Filename,ThreadNumber) ->
    Ref = make_ref(),
    {ok,Connection} = esqlite3_nif:open(Ref, self(), Filename,ThreadNumber),
    case receive_answer(Ref) of
        {ok,[]} ->
            {ok, {connection, make_ref(),Connection}};
        ok ->
            {ok, {connection, make_ref(),Connection}};
        {error, _Msg}=Error ->
            Error
    end.
open(Filename,ThreadNumber,Sql) ->
    Ref = make_ref(),
    {ok,Connection} = esqlite3_nif:open(Ref, self(), Filename,ThreadNumber,Sql),
    case receive_answer(Ref) of
        {ok,Res} ->
            {ok, {connection, make_ref(),Connection},Res};
        ok ->
            {ok, {connection, make_ref(),Connection}};
        {error, _Msg}=Error ->
            Error
    end.

-spec init(Threads) -> ok when
    Threads :: integer() | {integer(), tuple()}.
init(Threads) ->
    esqlite3_nif:init(Threads).

%% @doc Read number of pages in wal
-spec wal_pages(connection()) -> tuple().
wal_pages({connection, _Ref, Connection}) ->
    esqlite3_nif:wal_pages(Connection).

-spec fetchone(statement()) -> tuple().
fetchone(Statement) ->
    case try_step(Statement, 0) of
        '$done' -> ok;
        {row, Row} -> Row
    end.

%% 
-spec fetchall(statement()) -> list(tuple()).
fetchall(Statement) ->
    case try_step(Statement, 0) of
        '$done' -> 
            [];
        {row, Row} ->
            [Row | fetchall(Statement)]
    end.  

%% Try the step, when the database is busy, 
-spec try_step(statement(), non_neg_integer()) -> term().
try_step(_Statement, Tries) when Tries > 5 ->
    throw(too_many_tries);
try_step(Statement, Tries) ->
    case esqlite3:step(Statement) of
        '$busy' -> 
            timer:sleep(100 * Tries),
            try_step(Statement, Tries + 1);
        Something -> 
            Something
    end.
            
%% @doc Execute Sql statement, returns the number of affected rows.
%%
%% @spec exec(iolist(), connection()) -> integer() |  {error, error_message()}
% exec(Sql,  {connection, _Ref, Connection}) ->
%     Ref = make_ref(),
%     ok = esqlite3_nif:exec(Connection, Ref, self(), add_eos(Sql)),
%     receive_answer(Ref).

noop({connection, _Ref, Connection}) ->
    Ref = make_ref(),
    ok = esqlite3_nif:noop(Connection, Ref, self()),
    receive_answer(Ref).

bind_insert(Sql, [[_|_]|_] = Params, {connection, _Ref, Connection}) ->
    Ref = make_ref(),
    ok = esqlite3_nif:bind_insert(Connection,Ref,self(),Sql,Params),
    receive_answer(Ref).

%% @doc Execute Sql statement, returns: {changes,LastRowid,NumChanges} | {Columns,Rows} | ok | {error, reason()}
%% Rows will be in reverse order.
%% @spec exec(iolist(), connection()) -> integer() |  {error, error_message()}
exec_script(Sql, Db) ->
    exec_script(Sql,Db,infinity,0,0,<<>>).
exec_script(Sql,[_|_] = Recs, Db) ->
    exec_script(Sql,Recs,Db,infinity,0,0,<<>>);
exec_script(Sql, Db, Timeout) when is_integer(Timeout) ->
    exec_script(Sql,Db,Timeout,0,0,<<>>).
exec_script(Sql, [_|_] = Recs, Db, Timeout) when is_integer(Timeout) ->
    exec_script(Sql,Recs,Db,Timeout,0,0,<<>>).
exec_script(Sql,  {connection, _Ref, Connection},Timeout,Term,Index,AppendParam) ->
    Ref = make_ref(),
    ok = esqlite3_nif:exec_script(Connection, Ref, self(), Sql,Term,Index,AppendParam),
    receive_answer(Ref,Connection,Timeout).
exec_script(Sql, [_|_] = Recs, {connection, _Ref, Connection},Timeout,Term,Index,AppendParam) ->
    Ref = make_ref(),
    ok = esqlite3_nif:exec_script(Connection, Ref, self(), Sql,Term,Index,AppendParam,Recs),
    receive_answer(Ref,Connection,Timeout).

backup_init({connection, _, Dest},{connection, _, Src}) ->
    Ref = make_ref(),
    ok = esqlite3_nif:backup_init(Dest,Src,Ref,self()),
    case receive_answer(Ref) of
        {ok,B} ->
            {ok,{backup,make_ref(),B}};
        error ->
            error
    end.

backup_step({backup, _Ref, B},N) ->
    Ref = make_ref(),
    ok = esqlite3_nif:backup_step(B,Ref,self(),N),
    receive_answer(Ref).

backup_finish({backup, _Ref, B}) ->
    Ref = make_ref(),
    ok = esqlite3_nif:backup_finish(B,Ref,self()),
    receive_answer(Ref).

backup_pages({backup, _Ref, B}) ->
    esqlite3_nif:backup_pages(B).

%% @doc Close the database
%%
%% @spec close(connection()) -> ok | {error, error_message()}
-spec close(connection()) -> ok | {error, _}.
close( {connection, _Ref, _Connection}) ->
    ok.

receive_answer(Ref) ->
    receive 
        {Ref, Resp} -> Resp
    end.
receive_answer(Ref,Connection,Timeout) ->
    receive
        {Ref,Resp} ->
            Resp
    after Timeout ->
        ok = esqlite3_nif:interrupt_query(Connection),
        receive
            {Ref,Resp} ->
                Resp
        end
   end.

