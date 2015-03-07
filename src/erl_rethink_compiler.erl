-module(erl_rethink_compiler).

-export([
         compile/1
        ]).

-define(DATUM, 1).
-define(DB_LIST, 59).
-define(LIMIT, 71).

compile(db_list) ->
    [?DB_LIST];
compile({limit, Sequence, Count}) ->
    [?LIMIT, [compile(Sequence), Count]];

compile(_) ->
    error.
