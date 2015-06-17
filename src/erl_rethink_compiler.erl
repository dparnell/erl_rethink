-module(erl_rethink_compiler).

-export([
         compile/1
        ]).

-define(DATUM, 1).
-define(MAKE_ARRAY, 2).
-define(VAR, 10).
-define(DB, 14).
-define(TABLE, 15).
-define(GET, 16).
-define(EQ, 17).
-define(NE, 18).
-define(LT, 19).
-define(LE, 20).
-define(GT, 21).
-define(GE, 22).
-define(NOT, 23).
-define(GET_FIELD, 31).
-define(FILTER, 39).
-define(COUNT, 43).
-define(DB_CREATE, 57).
-define(DB_LIST, 59).
-define(TABLE_CREATE, 60).
-define(TABLE_LIST, 62).
-define(OR, 66).
-define(AND, 67).
-define(FUNC, 69).
-define(LIMIT, 71).
-define(DEFAULT, 92).
-define(CONTAINS, 93).
-define(BRACKET, 170).

-record(state, {
          variable_counter,
          variables
         }).

compile(Query) ->
    compile(Query, #state{
              variable_counter = 0,
              variables = []
             }).

add_new_variable(State, Name) ->
    VarNum = State#state.variable_counter + 1,
    Variables = lists:append([{Name, VarNum}], State#state.variables),

    {ok, State#state{variable_counter = VarNum, variables = Variables}, VarNum}.

add_new_variables(State, Variables) ->
    lists:foldl(fun(X, {State3, Vars}) ->
                        {ok, State4, VarNum} = add_new_variable(State3, X),
                        {State4, [VarNum | Vars]}
                end, {State, []}, Variables).

find_variable(State, Name) ->
    VarNum = proplists:get_value(Name, State#state.variables),
    case VarNum of
        undefined -> erlang:error({unknown_variable, Name});
        _ -> VarNum
    end.

compile(Value, _State) when is_binary(Value) ->
    Value;
compile(Value, _State) when is_number(Value) ->
    Value;
compile(null, _State) ->
    null;
compile({datum, Datum}, State) ->
    [?DATUM, compile(Datum, State)];
compile({array, Elements}, State) ->
    [?MAKE_ARRAY, [compile(X, State) || X <- Elements]];
compile({db, Database}, _State) ->
    [?DB, [Database]];
compile({table_create, Name}, _State) ->
    [?TABLE_CREATE, [Name]];
compile({table_create, Database, Name}, State) ->
    [?TABLE_CREATE, [compile(Database, State), Name]];
compile({table_list, Database}, State) ->
    [?TABLE_LIST, [compile(Database, State)]];
compile(table_list, _State) ->
    [?TABLE_LIST];
compile({db_create, Name}, _State) ->
    [?DB_CREATE, [Name]];
compile(db_list, _State) ->
    [?DB_LIST];
compile({limit, Sequence, Count}, State) ->
    [?LIMIT, [compile(Sequence, State), Count]];
compile({table, Table}, _State) ->
    [?TABLE, [Table]];
compile({table, Database, Table}, State) ->
    [?TABLE, [compile(Database, State), Table]];
compile({filter, Sequence, Condition}, State) ->
    [?FILTER, [compile(Sequence, State), compile(Condition, State)]];
compile({func, Params, Body}, State) ->
    {State2, VarNums} = add_new_variables(State, Params),
    [?FUNC, [[?MAKE_ARRAY, VarNums], compile(Body, State2)]];
compile({var, Variable}, State) ->
    [?VAR, [find_variable(State, Variable)]];
compile({eq, A, B}, State) ->
    [?EQ, [compile(A, State), compile(B, State)]];
compile({ne, A, B}, State) ->
    [?NE, [compile(A, State), compile(B, State)]];
compile({lt, A, B}, State) ->
    [?LT, [compile(A, State), compile(B, State)]];
compile({le, A, B}, State) ->
    [?LE, [compile(A, State), compile(B, State)]];
compile({gt, A, B}, State) ->
    [?GT, [compile(A, State), compile(B, State)]];
compile({ge, A, B}, State) ->
    [?GE, [compile(A, State), compile(B, State)]];
compile({invert, Value}, State) ->
    [?NOT, [compile(Value, State)]];
compile({all, Terms}, State) ->
    [?AND, [compile(X, State) || X <- Terms]];
compile({any, Terms}, State) ->
    [?OR, [compile(X, State) || X <- Terms]];
compile({count, Sequence}, State) ->
    [?COUNT, [compile(Sequence, State)]];
compile({default, A, B}, State) ->
    [?DEFAULT, [compile(A, State), compile(B, State)]];
compile({contains, A, B}, State) ->
    [?CONTAINS, [compile(A, State), compile(B, State)]];
compile({get_field, Object, Field}, State) ->
    [?BRACKET, [compile(Object, State), compile(Field, State)]];
compile(Query, _State) ->
    erlang:error({unhandled_term, Query}).
