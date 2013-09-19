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
         open/1,open/2,
         exec/2,
         exec_script/2,
         prepare/2, 
         step/1, 
         bind/2, 
         fetchone/1,
         fetchall/1,
         column_names/1,
         close/1]).

-export([q/2, q/3, map/3, foreach/3]).

%% 
-type connection() :: tuple().
-type statement() :: term().
-type sql() :: iolist().


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
        ok ->
            {ok, {connection, make_ref(),Connection}};
        {error, _Msg}=Error ->
            Error
    end.

-spec init(Threads) -> ok when
    Threads :: integer().
init(Threads) ->
    esqlite3_nif:init(Threads).

%% @doc Execute a sql statement, returns a list with tuples.
-spec q(sql(), connection()) -> list(tuple()).
q(Sql, Connection) ->
    q(Sql, [], Connection).

%% @doc Execute statement, bind args and return a list with tuples as result.
-spec q(sql(), list(), connection()) -> list(tuple()).
q(Sql, [], Connection) ->
    case prepare(Sql, Connection) of
        {ok, Statement} -> 
            fetchall(Statement);
        {error, _Msg}=Error -> 
            throw(Error)
    end;
q(Sql, Args, Connection) ->
    case prepare(Sql, Connection) of
        {ok, Statement} ->
            ok = bind(Statement, Args),
            fetchall(Statement);
        {error, _Msg}=Error -> 
            throw(Error)
    end.

%% @doc
-spec map(F, sql(), connection()) -> list(Type) when
    F :: fun((Row) -> Type) | fun((ColumnNames, Row) -> Type),
    Row :: tuple(),
    ColumnNames :: tuple(),
    Type :: any(). 
map(F, Sql, Connection) ->
    case prepare(Sql, Connection) of
        {ok, Statement} ->
            map_s(F, Statement);
        {error, _Msg}=Error -> 
            throw(Error)
    end.

%% @doc
-spec foreach(F, sql(), connection()) -> ok when
    F :: fun((Row) -> any()) | fun((ColumnNames, Row) -> any()),
    Row :: tuple(),
    ColumnNames :: tuple().
foreach(F, Sql, Connection) ->
    case prepare(Sql, Connection) of
        {ok, Statement} ->
            foreach_s(F, Statement);
        {error, _Msg}=Error ->
            throw(Error)
    end.

%%
-spec foreach_s(F, statement()) -> ok when
    F :: fun((Row) -> any()) | fun((ColumnNames, Row) -> any()),
    Row :: tuple(),
    ColumnNames :: tuple().
foreach_s(F, Statement) when is_function(F, 1) -> 
    case try_step(Statement, 0) of
        '$done' -> ok;
        {row, Row} ->
            F(Row),
            foreach_s(F, Statement)
    end;
foreach_s(F, Statement) when is_function(F, 2) ->
    ColumnNames = column_names(Statement),
    case try_step(Statement, 0) of
        '$done' -> ok;
        {row, Row} -> 
            F(ColumnNames, Row),
            foreach_s(F, Statement)
    end.

%%
-spec map_s(F, statement()) -> list(Type) when
    F :: fun((Row) -> Type) | fun((ColumnNames, Row) -> Type),
    Row :: tuple(),
    ColumnNames :: tuple(),
    Type :: term().
map_s(F, Statement) when is_function(F, 1) ->
    case try_step(Statement, 0) of
        '$done' -> [];
        {row, Row} -> 
            [F(Row) | map_s(F, Statement)]
    end;
map_s(F, Statement) when is_function(F, 2) ->
    ColumnNames = column_names(Statement),
    case try_step(Statement, 0) of
        '$done' -> [];
        {row, Row} -> 
            [F(ColumnNames, Row) | map_s(F, Statement)]
    end.

%%
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
exec(Sql,  {connection, _Ref, Connection}) ->
    Ref = make_ref(),
    ok = esqlite3_nif:exec(Connection, Ref, self(), add_eos(Sql)),
    receive_answer(Ref).

noop({connection, _Ref, Connection}) ->
    Ref = make_ref(),
    ok = esqlite3_nif:noop(Connection, Ref, self()),
    receive_answer(Ref).

%% @doc Execute Sql statement, returns: {rowid,Rowid} | {Columns,Rows} | ok | {error, reason()}
%% Rows will be in reverse order.
%% @spec exec(iolist(), connection()) -> integer() |  {error, error_message()}
exec_script(Sql,  {connection, _Ref, Connection}) ->
    Ref = make_ref(),
    ok = esqlite3_nif:exec_script(Connection, Ref, self(), Sql),
    receive_answer(Ref).

%% @doc Prepare a statement
%%
%% @spec prepare(iolist(), connection()) -> {ok, prepared_statement()} | {error, error_message()}
prepare(Sql,  {connection, _Ref, Connection}) ->
    Ref = make_ref(),
    ok = esqlite3_nif:prepare(Connection, Ref, self(), add_eos(Sql)),
    receive_answer(Ref).

%% @doc Step
%%
%% @spec step(prepared_statement()) -> tuple()
step(Stmt) ->
    Ref = make_ref(),
    ok = esqlite3_nif:step(Stmt, Ref, self()),
    receive_answer(Ref).

%% @doc Bind values to prepared statements
%%
%% @spec bind(prepared_statement(), value_list()) -> ok | {error, error_message()}
bind(Stmt, Args) ->
    Ref = make_ref(),
    ok = esqlite3_nif:bind(Stmt, Ref, self(), Args),
    receive_answer(Ref).

%% @doc Return the column names of the prepared statement.
%%
-spec column_names(statement()) -> tuple(atom()).
column_names(Stmt) ->
    Ref = make_ref(),
    ok = esqlite3_nif:column_names(Stmt, Ref, self()),
    receive_answer(Ref).

%% @doc Close the database
%%
%% @spec close(connection()) -> ok | {error, error_message()}
-spec close(connection()) -> ok | {error, _}.
close( {connection, _Ref, Connection}) ->
    Ref = make_ref(),
    ok = esqlite3_nif:close(Connection, Ref, self()),
    receive_answer(Ref).

%% Internal functions
add_eos(IoList) ->
    [IoList, 0].

receive_answer(Ref) ->
    receive 
        {Ref, Resp} -> Resp
    end.
