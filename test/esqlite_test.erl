%%
%% Test suite for esqlite.
%%

-module(esqlite_test).

-include_lib("eunit/include/eunit.hrl").

% interrupt_test() ->
%     esqlite3:init(2),
%     {ok, Db} = esqlite3:open("../user"),
%     {error,query_aborted} = esqlite3:exec_script("delete from t_dir "++"where eid > 2800 and eid < 3900;",Db,1000),
%     ok.

open_single_database_test() ->
    esqlite3:init(2),
    {ok, _C1} = esqlite3:open("test.db"),
    ok.

open_with_sql_test() ->
    esqlite3:init(2),
    {ok, _C1,[[{columns,_},{rows,[]}]]} = esqlite3:open("test.db",1,<<"select name, sql from sqlite_master where type='table';">>),
    ok.

% open_multiple_same_databases_test() ->
%     {ok, _C1} = esqlite3:open("test.db"),
%     {ok, _C2} = esqlite3:open("test.db"),
%     ok.

open_multiple_different_databases_test() ->
    esqlite3:init(2),
    {ok, _C1} = esqlite3:open("test1.db"),
    {ok, _C2} = esqlite3:open("test2.db"),
    ok.

simple_query_test() ->
    esqlite3:init(2),
    {ok, Db} = esqlite3:open(":memory:"),
    ok = esqlite3:exec("begin;", Db),
    ok = esqlite3:exec("create table test_table(one varchar(10), two int);", Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello1\"", ",", "10" ");"], Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello2\"", ",", "11" ");"], Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello3\"", ",", "12" ");"], Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello4\"", ",", "13" ");"], Db),
    ok = esqlite3:exec("commit;", Db),
    ok = esqlite3:exec("select * from test_table;", Db),
    ok.

prepare_test() ->
    esqlite3:init(2),
    {ok, Db} = esqlite3:open(":memory:"),
    esqlite3:exec("begin;", Db),
    esqlite3:exec("create table test_table(one varchar(10), two int);", Db),
    {ok, Statement} = esqlite3:prepare("insert into test_table values(\"one\", 2)", Db),
    
    '$done' = esqlite3:step(Statement),

    ok = esqlite3:exec(["insert into test_table values(", "\"hello4\"", ",", "13" ");"], Db), 

    %% Check if the values are there.
    [{<<"one">>, 2}, {<<"hello4">>, 13}] = esqlite3:q("select * from test_table order by two", Db),
    esqlite3:exec("commit;", Db),
    esqlite3:close(Db),

    ok.

bind_test() ->
    esqlite3:init(2),
    {ok, Db} = esqlite3:open(":memory:"),
    
    ok = esqlite3:exec("begin;", Db),
    ok = esqlite3:exec("create table test_table(one varchar(10), two int);", Db),
    ok = esqlite3:exec("commit;", Db),

    %% Create a prepared statement
    {ok, Statement} = esqlite3:prepare("insert into test_table values(?1, ?2)", Db),
    esqlite3:bind(Statement, [one, 2]),
    esqlite3:step(Statement),
    esqlite3:bind(Statement, ["three", 4]), 
    esqlite3:step(Statement),
    esqlite3:bind(Statement, ["five", 6]), 
    esqlite3:step(Statement),
    esqlite3:bind(Statement, [[<<"se">>, $v, "en"], 8]), % iolist bound as text
    esqlite3:step(Statement),
    esqlite3:bind(Statement, [<<"nine">>, 10]), % iolist bound as text
    esqlite3:step(Statement),
    esqlite3:bind(Statement, [{blob, [<<"eleven">>, 0]}, 12]), % iolist bound as blob with trailing eos.
    esqlite3:step(Statement),

    %% utf-8
    esqlite3:bind(Statement, [[<<228,184,138,230,181,183>>], 100]), 
    esqlite3:step(Statement),

    ?assertEqual([{<<"one">>, 2}], 
        esqlite3:q("select one, two from test_table where two = '2'", Db)),
    ?assertEqual([{<<"three">>, 4}], 
        esqlite3:q("select one, two from test_table where two = 4", Db)),
    ?assertEqual([{<<"five">>, 6}], 
        esqlite3:q("select one, two from test_table where two = 6", Db)),
    ?assertEqual([{<<"seven">>, 8}], 
        esqlite3:q("select one, two from test_table where two = 8", Db)),
    ?assertEqual([{<<"nine">>, 10}], 
        esqlite3:q("select one, two from test_table where two = 10", Db)),
    ?assertEqual([{{blob,<<$e,$l,$e,$v,$e,$n,0>>}, 12}], 
        esqlite3:q("select one, two from test_table where two = 12", Db)),

    %% utf-8
    ?assertEqual([{<<228,184,138,230,181,183>>, 100}], 
        esqlite3:q("select one, two from test_table where two = 100", Db)),

    ok.

bind_for_queries_test() ->
    esqlite3:init(2),
    {ok, Db} = esqlite3:open(":memory:"),
    
    ok = esqlite3:exec("begin;", Db),
    ok = esqlite3:exec("create table test_table(one varchar(10), two int);", Db),
    ok = esqlite3:exec("commit;", Db),

    ?assertEqual([{1}], esqlite3:q(<<"SELECT count(type) FROM sqlite_master WHERE type='table' AND name=?;">>, 
                [test_table], Db)),
    ?assertEqual([{1}], esqlite3:q(<<"SELECT count(type) FROM sqlite_master WHERE type='table' AND name=?;">>, 
                ["test_table"], Db)),
    ?assertEqual([{1}], esqlite3:q(<<"SELECT count(type) FROM sqlite_master WHERE type='table' AND name=?;">>, 
                [<<"test_table">>], Db)),
    ?assertEqual([{1}], esqlite3:q(<<"SELECT count(type) FROM sqlite_master WHERE type='table' AND name=?;">>, 
                [[<<"test_table">>]], Db)),

    ok.

column_names_test() ->
    esqlite3:init(2),
    {ok, Db} = esqlite3:open(":memory:"),
    ok = esqlite3:exec("begin;", Db),
    ok = esqlite3:exec("create table test_table(one varchar(10), two int);", Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello1\"", ",", "10" ");"], Db),
    ok = esqlite3:exec("commit;", Db),

    {ok, Stmt} = esqlite3:prepare("select * from test_table", Db),
    
    {one, two} = esqlite3:column_names(Stmt),
    
    ok.

foreach_test() ->
    esqlite3:init(2),
    {ok, Db} = esqlite3:open(":memory:"),
    ok = esqlite3:exec("begin;", Db),
    ok = esqlite3:exec("create table test_table(one varchar(10), two int);", Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello1\"", ",", "10" ");"], Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello2\"", ",", "11" ");"], Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello3\"", ",", "12" ");"], Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello4\"", ",", "13" ");"], Db),
    ok = esqlite3:exec("commit;", Db),

    F = fun(Row) ->
		case Row of 
		    {Key, Value} ->
			put(Key, Value);
		    _ ->
			ok
		end
	end,
    
    esqlite3:foreach(F, "select * from test_table;", Db),
    
    10 = get(<<"hello1">>),
    11 = get(<<"hello2">>),
    12 = get(<<"hello3">>), 
    13 = get(<<"hello4">>),
    
    ok.

backup_test() ->
    esqlite3:init(2),
    {ok, DbSrc} = esqlite3:open(":memory:"),
    {ok, DbDst} = esqlite3:open(":memory:"),
    {ok,_} = esqlite3:exec_script(["create table tab (id integer, val text);",
                                         "insert into tab values (1,'aaa');"
                                         "insert into tab values (2,'bbb');"],DbSrc),
    {ok,[[_,{rows,[_,_]}]]} = esqlite3:exec_script("select * from tab",DbSrc),
    {ok,Backup} = esqlite3:backup_init(DbDst,DbSrc),
    done = esqlite3:backup_step(Backup,100),
    ok = esqlite3:backup_finish(Backup),
    {ok,[[_,{rows,[_,_]}]]} = esqlite3:exec_script("select * from tab",DbDst),
    ok.

map_test() ->
    esqlite3:init(2),
    {ok, Db} = esqlite3:open(":memory:"),
    ok = esqlite3:exec("begin;", Db),
    ok = esqlite3:exec("create table test_table(one varchar(10), two int);", Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello1\"", ",", "10" ");"], Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello2\"", ",", "11" ");"], Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello3\"", ",", "12" ");"], Db),
    ok = esqlite3:exec(["insert into test_table values(", "\"hello4\"", ",", "13" ");"], Db),
    ok = esqlite3:exec("commit;", Db),

    F = fun(Row) -> Row end,
    
    [{<<"hello1">>,10},{<<"hello2">>,11},{<<"hello3">>,12},{<<"hello4">>,13}] 
        = esqlite3:map(F, "select * from test_table", Db),

    %% Test that when the row-names are added..
    Assoc = fun(Names, Row) -> 
		    lists:zip(tuple_to_list(Names), tuple_to_list(Row))
	    end,

    [[{one,<<"hello1">>},{two,10}],
     [{one,<<"hello2">>},{two,11}],
     [{one,<<"hello3">>},{two,12}],
     [{one,<<"hello4">>},{two,13}]]  = esqlite3:map(Assoc, "select * from test_table", Db),
    
    ?assertMatch({ok,[[{columns,{<<"one">>,<<"two">>}},
      {rows,[{<<"hello4">>,13},
             {<<"hello3">>,12},
             {<<"hello2">>,11},
             {<<"hello1">>,10}]}]]},esqlite3:exec_script(";;;;;select * from test_table;;;;;",Db)),
    ?assertMatch({ok,[{changes,5,1},
           [{columns,{<<"one">>,<<"two">>}},
            {rows,[{<<"hello4">>,13},
                   {<<"hello3">>,12},
                   {<<"hello2">>,11},
                   {<<"hello1">>,10}]}]]},
            esqlite3:exec_script("$savepoint 'adb';SELECT * FROM test_table;insert into test_table values ('exec2',100);$release savepoint 'adb';",Db)),

    ok.

error1_msg_test() ->
    esqlite3:init(2),
    % {ok, Db} = esqlite3:open(":memory:"),
    
    % %% Not sql.
    % {error, {sqlite_error, _Msg1}} = esqlite3:exec("dit is geen sql", Db),
    
    % %% Database test does not exist.
    % {error, {sqlite_error, _Msg2}} = esqlite3:exec("select * from test;", Db),
    
    % %% Opening non-existant database.
    % {error, {cantopen, _Msg3}} = esqlite3:open("/dit/bestaat/niet"),
    ok.
    
    

    

    
    

    
    
    
