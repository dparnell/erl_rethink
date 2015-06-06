-module(erl_rethink_compiler).

-export([
         compile/1
        ]).

-define(DATUM, 1).
-define(DB, 14).
-define(TABLE, 15).
-define(DB_LIST, 59).
-define(TABLE_LIST, 62).
-define(LIMIT, 71).

compile({datum, Datum}) ->
    [?DATUM, compile(Datum)];
compile({db, Database}) ->
    [?DB, [Database]];
compile({table_list, Database}) ->
    [?TABLE_LIST, [compile(Database)]];
compile(table_list) ->
    [?TABLE_LIST];
compile(db_list) ->
    [?DB_LIST];
compile({limit, Sequence, Count}) ->
    [?LIMIT, [compile(Sequence), Count]];
compile({table, Table}) ->
    [?TABLE, [Table]];
compile({table, Database, Table}) ->
    [?TABLE, [compile(Database), Table]];

compile(_) ->
    erlang:error(unhandled_term).
