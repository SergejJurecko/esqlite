%%
%% Test suite for esqlite.
%%

-module(esqlite_test).

-include_lib("eunit/include/eunit.hrl").
-define(BIGNUM,18446744073709551610).

% interrupt_test() ->
%     esqlite3:init(2),
%     {ok, Db} = esqlite3:open("../user"),
%     {error,query_aborted} = esqlite3:exec_script("delete from t_dir "++"where eid > 2800 and eid < 3900;",Db,1000),
%     ok.



wal_test() ->
    % spawn(fun() -> receive_wal() end),
    % timer:sleep(100),
    {ok,LS} = gen_tcp:listen(23244,[binary,{ip,{127,0,0,1}},{keepalive,true},{reuseaddr,true},{packet,4},{active,true}]),
    
    esqlite3:init(2),
    put(walframes,[]),
    InitBin = "HELLO REPLICATOR!",
    % Both threads will be waiting to accept connection
    ok = esqlite3:tcp_connect("127.0.0.1",23244,InitBin,0),
    ok = esqlite3:tcp_connect("127.0.0.1",23244,InitBin,1),
    
    {ok,S1} = gen_tcp:accept(LS),
    {ok,S2} = gen_tcp:accept(LS),
    inet:setopts(S1,[{active,true},{packet,4}]), 
    inet:setopts(S2,[{active,true},{packet,4}]), 

    % Get hello
    rec_wal_sock(S1,0),
    rec_wal_sock(S2,0),

    Nm = "waltest",
    % case filelib:file_size(Nm) > 0 of
    %     true ->
    %         {ok, Db1,_} = esqlite3:open("waltest",0,["PRAGMA journal_mode=wal;PRAGMA journal_size_limit=0;PRAGMA synchronous=0;"]),
    %         esqlite3:close(Db1);
    %     _ ->
    %         ok
    % end,
    file:delete(Nm),
    {ok, Db,_Res} = esqlite3:open(Nm,0,["PRAGMA journal_mode=wal;PRAGMA journal_size_limit=0;",
                                                "PRAGMA synchronous=0;PRAGMA page_size=4096;"]),
    ok = esqlite3:replicate_opts(Db,"PREFIX"),

    rec_wal_sock(S1,0),
    rec_wal_sock(S2,0),

    {ok,_} = esqlite3:exec_script("CREATE TABLE tab (id INTEGER PRIMARY KEY, val TEXT);",Db,
                infinity,?BIGNUM,?BIGNUM,<<"Variable">>),
    rec_wal_sock(S1,?BIGNUM),
    rec_wal_sock(S2,?BIGNUM),
    {ok,_} = esqlite3:exec_script("INSERT INTO tab VALUES (1,'aaa');",Db,
        infinity,?BIGNUM-1,?BIGNUM-1,<<"Variable">>),
    rec_wal_sock(S1,?BIGNUM-1),
    rec_wal_sock(S2,?BIGNUM-1),
    {ok,_} = esqlite3:exec_script("INSERT INTO tab VALUES (2,'aaa');",Db,
        infinity,?BIGNUM-2,?BIGNUM-2,<<"Variable">>),
    rec_wal_sock(S1,?BIGNUM-2),
    rec_wal_sock(S2,?BIGNUM-2),
    {ok,_} = esqlite3:exec_script("INSERT INTO tab VALUES (3,'aaa');",Db,
        infinity,?BIGNUM-3,?BIGNUM-3,<<"Variable">>),
    rec_wal_sock(S1,?BIGNUM-3),
    rec_wal_sock(S2,?BIGNUM-3),
    {ok,_} = esqlite3:exec_script("INSERT INTO tab VALUES (4,'aaa');",Db,
        infinity,?BIGNUM-4,?BIGNUM-4,<<"Variable">>),
    rec_wal_sock(S1,?BIGNUM-4),
    rec_wal_sock(S2,?BIGNUM-4),

    % gen_tcp:close(S1),
    % gen_tcp:close(S2),

    % esqlite3:tcp_reconnect(),
    % {ok,S11} = gen_tcp:accept(LS),
    % {ok,S22} = gen_tcp:accept(LS),
    % inet:setopts(S11,[{active,true},{packet,4}]), 
    % inet:setopts(S22,[{active,true},{packet,4}]), 

    {ok,_} = esqlite3:exec_script(["begin;INSERT INTO tab VALUES (5,'aaa');",
        "INSERT INTO tab VALUES (6,'",binary:copy(<<"a">>,100),"');"
                "commit;"],Db,
                infinity,?BIGNUM-5,?BIGNUM-5,<<"Variable">>),
    rec_wal_sock(S1,?BIGNUM-5),
    rec_wal_sock(S2,?BIGNUM-5),
    {ok,<<Header:32/binary,_WalBin/binary>>} = file:read_file(Nm++"-wal"),
    <<_:16/binary,Salt:8/binary,_/binary>> = Header,
    ?debugFmt("Wal before close ~p",[filelib:file_size(Nm++"-wal")]),
    esqlite3:close(Db),
    ?debugFmt("Wal after close ~p",[filelib:file_size(Nm++"-wal")]),
    {ok, Db1,_} = esqlite3:open(Nm,0,["PRAGMA journal_mode=wal;PRAGMA journal_size_limit=0;",
                                                "PRAGMA synchronous=0;PRAGMA page_size;"]),
    {ok,ReadResult} = esqlite3:exec_script("select * from tab;",Db1),
    ?debugFmt("After reopen ~p",[ReadResult]),
    esqlite3:close(Db1),
    duplicate_db(Salt,ReadResult),
    ok.

duplicate_db(_Salt,ReadResult) ->
    Nm = "waltest_copy",
    Header = esqlite3:make_wal_header(4096),
    file:write_file(Nm++"-wal",[Header|lists:reverse(get(walframes))],[write,binary]),
    {ok, Db,_Out} = esqlite3:open(Nm,0,["PRAGMA journal_mode=wal;PRAGMA journal_size_limit=0;",
                                                "PRAGMA synchronous=0;PRAGMA page_size;"]),
    case esqlite3:exec_script("select * from tab;",Db) of
        {ok,ReadResult} ->
            ok;
        {ok,SomethingElse} ->
            ?debugFmt("Duplicate returned ~p, should have ~p",[SomethingElse,ReadResult]),
            exit(error)
    end,
    esqlite3:close(Db),
    ok.

rec_wal_sock(S,Num) ->
    rec_wal_sock(0,S,100,Num).
rec_wal_sock(N,S,Timeout,Num) ->
    receive
        {tcp,S,<<"HELLO REPLICATOR!">>} ->
            % ?debugFmt("Received hello ~p",[S]),
            rec_wal_sock(N+1,S,0,Num);
        {tcp,S,<<>>} ->
            rec_wal_sock(N+1,S,Timeout,Num);
        {tcp,S,Data} ->
            ok = parse_page(Data,Num),
            rec_wal_sock(N+1,S,0,Num);
        {tcp_closed,S} ->
            closed
    after Timeout ->
        ok
    end.
parse_page(<<_LenPrefix:16/unsigned,"PREFIX",
             LenVarPrefix:16/unsigned,VarPrefix:LenVarPrefix/binary,
             LenHeader,Header:LenHeader/binary,
             LenPage:16,Page:LenPage/binary,Rem/binary>>,Num) ->
    case LenPage of
        0 ->
            ok;
        _ ->
            <<_PN:32,_DBSize:32,(Num):64/unsigned,(Num):64/unsigned,_/binary>> = Header,
            case VarPrefix of
                <<>> ->
                    ok;
                <<"Variable">> ->
                    ok
            end,
            put(walframes,[[Header,esqlite3:lz4_decompress(Page,4096)]|get(walframes)])
    end,
    parse_page(Rem,Num);
parse_page(<<>>,_) ->
    ok.

lz4_test() ->
    esqlite3:init(2),
    Bin1 = binary:copy(<<"SELECT * FROM WTF;">>,2),
    {Compressed1,CompressedSize1} = esqlite3:lz4_compress(Bin1),
    % ?debugFmt("Compressed ~p size ~p ",[byte_size(Compressed),CompressedSize]),
    Bin1 = esqlite3:lz4_decompress(Compressed1,byte_size(Bin1),CompressedSize1),
    ok.

open_single_database_test() ->
    esqlite3:init(2),
    {ok, _C1} = esqlite3:open("test.db"),
    ok.

open_with_sql_test() ->
    file:delete("test.db"),
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
    
    

    

    
    

    
    
    
