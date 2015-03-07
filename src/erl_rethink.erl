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
         test/0
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


test() ->
    {ok, C} = connect(),

    {ok, DbList} = run(C, db_list),
    io:format("DBList = ~p~n", [DbList]),

    {ok, Result} = run(C, {limit, db_list, 1}),
    io:format("Result = ~p~n", [Result]).
