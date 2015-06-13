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
         arun/3,
         acontinue/2,
         astop/2,
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

arun(Connection, Query, QueryState) ->
    gen_server:call(Connection, {arun, Query, QueryState}, 30000).

acontinue(Connection, Token) ->
    gen_server:cast(Connection, {acontinue, Token}).

astop(Connection, Token) ->
    gen_server:cast(Connection, {astop, Token}).

test() ->
    {ok, C} = connect(),

    io:format("DBList = ~p~n", [run(C, db_list)]),
    io:format("Limited DBList = ~p~n", [run(C, {limit, db_list, 1})]),
    io:format("table list should fail = ~p~n", [run(C, {table_list, {db, <<"blah blah">>}})]),
    io:format("table list = ~p~n", [run(C, table_list)]),

    ok = close(C).

handle_data() ->
    receive
        {rethinkdb, C, Token, {more, Response}} -> io:format("got partial response: ~p documents~n", [length(Response)]),
                                                   acontinue(C, Token),
                                                   handle_data();
        {rethinkdb, C, Token, {ok, Response}} -> io:format("got final response: ~p documents~n", [length(Response)]),
                                                 acontinue(C, Token);
        Message -> io:format("Unexpected: ~p~n", [Message])
    end.


atest() ->
    {ok, C} = connect(),

    {ok, Token} = arun(C, db_list),
    io:format("Sent query.  Token = ~p~n", [Token]),

    receive
        {rethinkdb, C, Token, Response} -> io:format("got response: ~p~n", [Response]);
        Message -> io:format("Unexpected: ~p~n", [Message])
    end,

    {ok, Token2} = arun(C, {table, {db, <<"clients">>}, <<"actions">>}),
    io:format("Query 2: ~p~n", [Token2]),

    handle_data(),

    ok = close(C).
