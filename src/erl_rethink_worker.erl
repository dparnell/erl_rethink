-module(erl_rethink_worker).
-behaviour(gen_server).

-export([
         start_link/1
        ]).

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-record(state, {
    socket,
    db,
    token
}).

-define(PROTOCOL_V3, 1601562686).
-define(JSON_FORMAT, 2120839367).

-define(START, 1).
-define(CONTINUE, 2).
-define(STOP, 3).
-define(NOREPLY_WAIT, 4).

-define(SUCCESS_ATOM, 1).
-define(SUCCESS_SEQUENCE, 2).
-define(SUCCESS_PARTIAL, 3).
-define(WAIT_COMPLETE, 4).
-define(CLIENT_ERROR, 16).
-define(COMPILE_ERROR, 17).
-define(RUNTIME_ERROR, 18).

start_link(Options) ->
    gen_server:start_link(?MODULE, [Options], []).

%%% gen_server functions

init([{Host, Port}]) ->
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {packet, 0}, {active, false}]),
    case login(Socket) of
        ok ->
            State = #state{
              socket = Socket,
              db = <<"test">>,
              token = 0
             },
            {ok, State};
        Error -> Error
    end;

init([{Host, Port, Auth}]) ->
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {packet, 0}, {active, false}]),
    case login(Socket, Auth) of
        ok ->
            State = #state{
              socket = Socket,
              db = <<"test">>
             },
            {ok, State};
        Error -> Error
    end.

handle_call({run, Query}, _From, State) ->
    Compiled = erl_rethink_compiler:compile(Query),
    Socket = State#state.socket,
    Token = State#state.token,
%%    ok = send(Socket, Token, [?START, Compiled, [{}]]),
    ok = send(Socket, Token, [?START, Compiled]),
    Reply = recv(Socket),
    {reply, Reply, State#state{ token = State#state.token + 1 }};

handle_call(close, _From, State) ->
    State2 = close(State),
    {reply, ok, State2};
handle_call(_Params, _From, _State) ->
    {error}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_Message, State) ->
     {noreply, State}.

terminate(_Reason, State) ->
    close(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% Shutdown code

close_socket(undefined) ->
    ok;
close_socket(Socket) ->
    gen_tcp:close(Socket).

close(State) ->
    ok = close_socket(State#state.socket),
    State#state{ socket = undefined }.

%%% Login code

login(Socket, Auth) ->
    ok = gen_tcp:send(Socket, <<?PROTOCOL_V3:32/little-integer, (iolist_size(Auth)):32/little-integer, Auth/binary, ?JSON_FORMAT:32/little-integer>>),
    look_for_success(Socket).

login(Socket) ->
    ok = gen_tcp:send(Socket, <<?PROTOCOL_V3:32/little-integer, 0:32, ?JSON_FORMAT:32/little-integer>>),
    look_for_success(Socket).

look_for_success(Socket) ->
    look_for_success(Socket, []).

look_for_success(Socket, Acc) ->
    {ok, Chunk} = gen_tcp:recv(Socket, 0),
    Acc2 = [Acc, Chunk],

    case binary:at(Chunk, iolist_size(Chunk) - 1) == 0 of
        true -> check_login_result(iolist_to_binary(Acc2));
        false -> look_for_success(Socket, Acc2)
    end.

check_login_result(<<"SUCCESS", 0>>) ->
    ok;
check_login_result(Message) ->
    {error, Message}.

%%% query send and receive code

send(Socket, Token, Data) ->
    Json = jsx:encode(Data),
    Length = iolist_size(Json),
    %%io:format("JSON: ~p~n", [Json]),
    gen_tcp:send(Socket, [<<Token:64/little-unsigned, Length:32/little-unsigned>>, Json]).

recv(Socket) ->
    {ok, _Token} = gen_tcp:recv(Socket, 8),
    {ok, Length} = gen_tcp:recv(Socket, 4),
    {ok, Chunk} = gen_tcp:recv(Socket, binary:decode_unsigned(Length, little)),
    parse_response(Chunk).

parse_response(Json) ->
    Resp = jsx:decode(Json),
    Type = proplists:get_value(<<"t">>, Resp),

    handle_response(Type, Resp).

handle_response(?SUCCESS_ATOM, Resp) ->
    [Result] = proplists:get_value(<<"r">>, Resp, undefined),
    {ok, Result};
handle_response(?SUCCESS_SEQUENCE, Resp) ->
    Result = proplists:get_value(<<"r">>, Resp, undefined),
    {ok, Result}.
