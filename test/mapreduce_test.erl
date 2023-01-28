-module(mapreduce_test).
-include_lib("eunit/include/eunit.hrl").

setup() -> 
    mapreduce_sup:start_link(),
    ok.

map_test_() ->
    {setup,
     fun setup/0,
     fun t1/0
    }.

t1() ->
    Map = fun({_K, V}) -> 
                  Pokemon = jsx:decode(V),
                  Types = maps:get(<<"types">>, Pokemon),
                  Data = #{<<"name">> => maps:get(<<"maps">>, Pokemon),
                           <<"height">> => maps:get(<<"height">>, Pokemon),
                           <<"weight">> => maps:get(<<"weight">>, Pokemon)},
                  lists:map(fun(T) ->
                                    TypeName = maps:get(<<"name">>, maps:get(<<"type">>, T)),
                                    {TypeName, jsx:encode(Data)}
                            end, Types)
          end,

    {ok, Pid} = mapper:start_link(),
    mapper:put_keys([
                     "test_1",
                     "test_2",
                     "test_4"]),
    mapper:put_function(Map),
    mapper:run(),

    timer:sleep(1000),

    ?assertEqual(
       sys:get_state(Pid), 
       {mapper_data, Map, ["test_1", "test_2", "test_4"], 
        ["test_2", "test_1"], ["test_4"]}).


