-module(erl_rethink_compiler).

-export([
         compile/1
        ]).

-record(state, {
          variable_counter,
          variables
         }).

-define(DATUM, 1).
-define(MAKE_ARRAY, 2).
-define(MAKE_OBJ, 3).
-define(VAR, 10).
-define(JAVASCRIPT, 11).
-define(UUID, 169).
-define(HTTP, 153).
-define(ERROR, 12).
-define(IMPLICIT_VAR, 13).
-define(DB, 14).
-define(TABLE, 15).
-define(GET, 16).
-define(GET_ALL, 78).
-define(EQ, 17).
-define(NE, 18).
-define(LT, 19).
-define(LE, 20).
-define(GT, 21).
-define(GE, 22).
-define(NOT, 23).
-define(ADD, 24).
-define(SUB, 25).
-define(MUL, 26).
-define(DIV, 27).
-define(MOD, 28).
-define(FLOOR, 183).
-define(CEIL, 184).
-define(ROUND, 185).
-define(APPEND, 29).
-define(PREPEND, 80).
-define(DIFFERENCE, 95).
-define(SET_INSERT, 88).
-define(SET_INTERSECTION, 89).
-define(SET_UNION, 90).
-define(SET_DIFFERENCE, 91).
-define(SLICE, 30).
-define(SKIP, 70).
-define(LIMIT, 71).
-define(OFFSETS_OF, 87).
-define(CONTAINS, 93).
-define(GET_FIELD, 31).
-define(KEYS, 94).
-define(OBJECT, 143).
-define(HAS_FIELDS, 32).
-define(WITH_FIELDS, 96).
-define(PLUCK, 33).
-define(WITHOUT, 34).
-define(MERGE, 35).
-define(BETWEEN_DEPRECATED, 36).
-define(BETWEEN, 182).
-define(REDUCE, 37).
-define(MAP, 38).
-define(FILTER, 39).
-define(CONCAT_MAP, 40).
-define(ORDER_BY, 41).
-define(DISTINCT, 42).
-define(COUNT, 43).
-define(IS_EMPTY, 86).
-define(UNION, 44).
-define(NTH, 45).
-define(BRACKET, 170).
-define(INNER_JOIN, 48).
-define(OUTER_JOIN, 49).
-define(EQ_JOIN, 50).
-define(ZIP, 72).
-define(RANGE, 173).
-define(INSERT_AT, 82).
-define(DELETE_AT, 83).
-define(CHANGE_AT, 84).
-define(SPLICE_AT, 85).
-define(COERCE_TO, 51).
-define(TYPE_OF, 52).
-define(UPDATE, 53).
-define(DELETE, 54).
-define(REPLACE, 55).
-define(INSERT, 56).
-define(DB_CREATE, 57).
-define(DB_DROP, 58).
-define(DB_LIST, 59).
-define(TABLE_CREATE, 60).
-define(TABLE_DROP, 61).
-define(TABLE_LIST, 62).
-define(CONFIG, 174).
-define(STATUS, 175).
-define(WAIT, 177).
-define(RECONFIGURE, 176).
-define(REBALANCE, 179).
-define(SYNC, 138).
-define(INDEX_CREATE, 75).
-define(INDEX_DROP, 76).
-define(INDEX_LIST, 77).
-define(INDEX_STATUS, 139).
-define(INDEX_WAIT, 140).
-define(INDEX_RENAME, 156).
-define(FUNCALL, 64).
-define(BRANCH, 65).
-define(OR, 66).
-define(AND, 67).
-define(FOR_EACH, 68).
-define(FUNC, 69).
-define(ASC, 73).
-define(DESC, 74).
-define(INFO, 79).
-define(MATCH, 97).
-define(UPCASE, 141).
-define(DOWNCASE, 142).
-define(SAMPLE, 81).
-define(DEFAULT, 92).
-define(JSON, 98).
-define(TO_JSON_STRING, 172).
-define(ISO8601, 99).
-define(TO_ISO8601, 100).
-define(EPOCH_TIME, 101).
-define(TO_EPOCH_TIME, 102).
-define(NOW, 103).
-define(IN_TIMEZONE, 104).
-define(DURING, 105).
-define(DATE, 106).
-define(TIME_OF_DAY, 126).
-define(TIMEZONE, 127).
-define(YEAR, 128).
-define(MONTH, 129).
-define(DAY, 130).
-define(DAY_OF_WEEK, 131).
-define(DAY_OF_YEAR, 132).
-define(HOURS, 133).
-define(MINUTES, 134).
-define(SECONDS, 135).
-define(TIME, 136).
-define(MONDAY, 107).
-define(TUESDAY, 108).
-define(WEDNESDAY, 109).
-define(THURSDAY, 110).
-define(FRIDAY, 111).
-define(SATURDAY, 112).
-define(SUNDAY, 113).
-define(JANUARY, 114).
-define(FEBRUARY, 115).
-define(MARCH, 116).
-define(APRIL, 117).
-define(MAY, 118).
-define(JUNE, 119).
-define(JULY, 120).
-define(AUGUST, 121).
-define(SEPTEMBER, 122).
-define(OCTOBER, 123).
-define(NOVEMBER, 124).
-define(DECEMBER, 125).
-define(LITERAL, 137).
-define(GROUP, 144).
-define(SUM, 145).
-define(AVG, 146).
-define(MIN, 147).
-define(MAX, 148).
-define(SPLIT, 149).
-define(UNGROUP, 150).
-define(RANDOM, 151).
-define(CHANGES, 152).
-define(ARGS, 154).
-define(BINARY, 155).
-define(GEOJSON, 157).
-define(TO_GEOJSON, 158).
-define(POINT, 159).
-define(LINE_TERM, 160).
-define(POLYGON, 161).
-define(DISTANCE, 162).
-define(INTERSECTS, 163).
-define(INCLUDES, 164).
-define(CIRCLE, 165).
-define(GET_INTERSECTING, 166).
-define(FILL, 167).
-define(GET_NEAREST, 168).
-define(POLYGON_SUB, 171).
-define(MINVAL, 180).
-define(MAXVAL, 181).

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
compile(true, _State) ->
    true;
compile(false, _State) ->
    false;
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
