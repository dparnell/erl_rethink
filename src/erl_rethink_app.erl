%% @private
-module(erl_rethink_app).
-behaviour(application).

%% API.
-export([start/2]).
-export([stop/1]).

%% API.
start(_Type, _Args) ->
    erl_rethink_sup:start_link().

stop(_State) ->
    ok.
