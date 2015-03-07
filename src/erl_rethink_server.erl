-module(erl_rethink_server).
-behaviour(gen_server).

-define(SERVER, ?MODULE).

-export([
         start_link/0
        ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%% gen_server functions

init([]) ->
    {ok, []}.

handle_call({connect, Host, Port}, _From, State) ->
    {ok, Pid} = erl_rethink_worker:start_link({Host, Port}),
    {reply, {ok, Pid}, State};
handle_call({connect, Host, Port, Auth}, _From, State) ->
    {ok, Pid} = erl_rethink_worker:start_link({Host, Port, Auth}),
    {reply, {ok, Pid}, State};

handle_call(_Params, _From, _State) ->
    {error}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_Message, State) ->
     {noreply, State}.

terminate(_Reason, _) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
