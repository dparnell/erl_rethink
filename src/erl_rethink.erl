-module(erl_rethink).

-export([
         start/0,
         connect/0,
         connect/1,
         connect/2,
         connect/3,
         close/1,
         run/3,
         run/2,
         arun/2,
         test/0,
         atest/0
        ]).

-define(SERVER, erl_rethink_server).

start() ->
    ok = application:start(inets),
    ok = application:start(jsx),
    ok = application:start(erl_rethink).

connect() ->
    connect("127.0.0.1").

connect(Host) ->
    connect(Host, 28015).

connect(Host, Port) ->
    gen_server:call(?SERVER, {connect, Host, Port}).

connect(Host, Port, Authorization) ->
    gen_server:call(?SERVER, {connect, Host, Port, Authorization}).

close(Connection) ->
    gen_server:call(Connection, close).

run(Connection, Query) ->
    run(Connection, Query, 30000).

run(Connection, Query, Timeout) ->
    gen_server:call(Connection, {run, Query}, Timeout).

arun(Connection, Query) ->
    gen_server:call(Connection, {arun, Query}, 30000).


test() ->
    {ok, C} = connect(),

    io:format("DBList = ~p~n", [run(C, db_list)]),
    io:format("Limited DBList = ~p~n", [run(C, {limit, db_list, 1})]),
    io:format("table list should fail = ~p~n", [run(C, {table_list, {db, <<"blah blah">>}})]),
    io:format("table list = ~p~n", [run(C, table_list)]),

    ok = close(C).

atest() ->
    {ok, C} = connect(),

    {ok, Token} = arun(C, db_list),
    io:format("Sent query.  Token = ~p~n", [Token]),

    receive
        {rethinkdb, C, Token, Response} -> io:format("got response: ~p~n", [Response]);
        Message -> io:format("Unexpected: ~p~n", [Message])
    end,

    ok = close(C).
