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
          token,
          reply_to,
          reply_state,
          reply_token,
          reply_bytes_needed,
          reply_bytes
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
              token = 0,
              reply_to = [],
              reply_state = header,
              reply_bytes_needed = 0,
              reply_bytes = []
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
              db = <<"test">>,
              token = 0,
              reply_to = [],
              reply_state = header,
              reply_bytes_needed = 0,
              reply_bytes = []
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
    {reply, Reply, State#state{ token = Token + 1 }};

handle_call({arun, Query}, From, State) ->
    Token = State#state.token,
    {ReplyTo, _} = From,
    gen_server:cast(self(), {arun, Query, Token}),
    State2 = State#state{ token = Token + 1, reply_to = [{Token, ReplyTo} | State#state.reply_to]},
    {reply, {ok, Token}, State2};

handle_call({arun, Query, QueryState}, From, State) ->
    Token = State#state.token,
    {ReplyTo, _} = From,
    gen_server:cast(self(), {arun, Query, Token}),
    State2 = State#state{ token = Token + 1, reply_to = [{Token, {ReplyTo, QueryState}} | State#state.reply_to]},
    {reply, {ok, Token}, State2};

handle_call(close, _From, State) ->
    State2 = close(State),
    {reply, ok, State2};
handle_call(_Params, _From, _State) ->
    {error, "Unhandled call"}.

handle_cast({arun, Query, Token}, State) ->
    Socket = State#state.socket,
    Compiled = erl_rethink_compiler:compile(Query),
    ok = send(Socket, Token, [?START, Compiled]),
    %% now we are running in async mode tell us when something comes in on the socket
    inet:setopts(Socket, [{active, once}]),

    {noreply, State};

handle_cast({acontinue, Token}, State) ->
    Socket = State#state.socket,
    ok = send(Socket, Token, [?CONTINUE]),
    %% now we are running in async mode tell us when something comes in on the socket
    inet:setopts(Socket, [{active, once}]),

    {noreply, State};

handle_cast({astop, Token}, State) ->
    Socket = State#state.socket,
    ok = send(Socket, Token, [?STOP]),
    %% now we are running in async mode tell us when something comes in on the socket
    inet:setopts(Socket, [{active, once}]),

    {noreply, State};

handle_cast(_, State) ->
    {noreply, State}.

handle_info({tcp, Socket, Data}, State) ->
    {noreply, State2} = process_data(Data, State),
    inet:setopts(Socket, [{active, once}]),
    {noreply, State2};

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
    {ok, Result};
handle_response(?SUCCESS_PARTIAL, Resp) ->
    Result = proplists:get_value(<<"r">>, Resp, undefined),
    {more, Result};
handle_response(?WAIT_COMPLETE, _Resp) ->
    ok;
handle_response(?CLIENT_ERROR, Resp) ->
    [Result] = proplists:get_value(<<"r">>, Resp, undefined),
    {client_error, Result};
handle_response(?COMPILE_ERROR, Resp) ->
    [Result] = proplists:get_value(<<"r">>, Resp, undefined),
    {compile_error, Result};
handle_response(?RUNTIME_ERROR, Resp) ->
    [Result] = proplists:get_value(<<"r">>, Resp, undefined),
    {error, Result}.

process_data(Data, State) ->
    Chunks = [State#state.reply_bytes, Data],
    process_chunks(State#state.reply_state, Chunks, State).

process_chunks(header, Chunks, State) ->
    Size = iolist_size(Chunks),
    if
        Size >= 12 ->
            found_header(Chunks, State);
        true -> State2 = State#state { reply_bytes = Chunks },
                {noreply, State2}
    end;
process_chunks(body, Chunks, State) ->
    Size = iolist_size(Chunks),
    BytesRequired = State#state.reply_bytes_needed,
    if
        Size =:= BytesRequired ->
            Response = parse_response(iolist_to_binary(Chunks)),
            dispatch_response(State#state.reply_token, Response, State);
        Size > BytesRequired ->
            AllBytes = iolist_to_binary(Chunks),
            ThisData = binary:part(AllBytes, 0, BytesRequired),
            NextData = binary:part(AllBytes, BytesRequired, Size - BytesRequired),
            Response = parse_response(ThisData),
            {_, State2} = dispatch_response(State#state.reply_token, Response, State),
            State3 = State2#state { reply_bytes = [], reply_state = header },
            process_data(NextData, State3);
        true ->
            State2 = State#state { reply_bytes = Chunks },
            {noreply, State2}
    end.

found_header(Chunks, State) ->
    Data = iolist_to_binary(Chunks),
    <<Token:64/little-unsigned, Length:32/little-unsigned, Rest/binary>> = Data,

    RestLength = byte_size(Rest),
    if
        RestLength =:= Length ->
            %% we got exactly the number of bytes we need
            Response = parse_response(Rest),
            dispatch_response(Token, Response, State);
        RestLength > Length ->
            %% we got more thank the number of bytes we need
            ThisData = binary:part(Rest, 0, Length),
            NextData = binary:part(Rest, Length, RestLength - Length),
            Response = parse_response(ThisData),
            {_, State2} = dispatch_response(Token, Response, State),
            State3 = State2#state { reply_bytes = [], reply_state = header },
            process_data(NextData, State3);
        true ->
            %% we got less than the number of bytes we need :(
            State2 = State#state { reply_token = Token, reply_bytes = [Rest], reply_state = body, reply_bytes_needed = Length },
            {noreply, State2}
    end.

update_reply_to(_Token, {more, _Result}, ReplyTargets) ->
    ReplyTargets;
update_reply_to(Token, _Response, ReplyTargets) ->
    proplists:delete(Token, ReplyTargets).

send_query_response(Token, Response, {TargetPID, QueryState}) ->
    TargetPID ! {rethinkdb, self(), Token, Response, QueryState};
send_query_response(Token, Response, TargetPID) ->
    TargetPID ! {rethinkdb, self(), Token, Response}.

dispatch_response(Token, Response, State) ->
    ReplyTargets = State#state.reply_to,
    ReplyTarget = proplists:get_value(Token, ReplyTargets),
    send_query_response(Token, Response, ReplyTarget),

    State2 = State#state { reply_token = undefined, reply_state = header, reply_bytes_needed = 0, reply_bytes = [], reply_to = update_reply_to(Token, Response, ReplyTargets) },

    {noreply, State2}.
