-module(mapper_test).
-include_lib("eunit/include/eunit.hrl").

setup() -> 
    mapreduce_sup:start_link(),
    ok.

map_test_() ->
    {setup,
     fun setup/0,
     [fun map_returns_single_value/0,
      fun map_returns_multiple_values/0]
    }.

map_returns_single_value() ->
    Inputs = ["pokemon_1", "pokemon_2", "pokemon_3"],
    Map = fun({K, V}) ->
                  Pokemon = jsx:decode(V),
                  Res = #{<<"name">> => maps:get(<<"name">>, Pokemon),
                          <<"height">> => maps:get(<<"height">>, Pokemon)},
                  {K, jsx:encode(Res)} 
          end,

    mapper:put_keys(Inputs),
    mapper:put_function(Map),
    mapper:run(),

    timer:sleep(1000),

    {mapper_data, Map, SourceInputs, Completed, _Failed} = sys:get_state(mapper),
    true = lists:all(fun(X) -> lists:member(X, Completed) end, SourceInputs).

map_returns_multiple_values() ->
    Inputs = ["pokemon_1", "pokemon_2", "pokemon_3"],
    Map = fun({_K, V}) -> 
                  Pokemon = jsx:decode(V),
                  Types = maps:get(<<"types">>, Pokemon),
                  Data = #{<<"name">> => maps:get(<<"name">>, Pokemon),
                           <<"height">> => maps:get(<<"height">>, Pokemon),
                           <<"weight">> => maps:get(<<"weight">>, Pokemon)},
                  lists:map(fun(T) ->
                                    TypeName = maps:get(<<"name">>, maps:get(<<"type">>, T)),
                                    {binary_to_list(TypeName), jsx:encode(Data)}
                            end, Types)
          end,
    mapper:put_keys(Inputs),
    mapper:put_function(Map),
    mapper:run(),

    timer:sleep(1000),


    {mapper_data, Map, SourceInputs, Completed, _Failed} = sys:get_state(mapper),
    true = lists:all(fun(X) -> lists:member(X, Completed) end, SourceInputs).
