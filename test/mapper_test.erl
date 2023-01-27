-module(mapper_test).
-include_lib("eunit/include/eunit.hrl").

setup() -> ok.

map_test_() ->
    {setup,
     fun setup/0,
     fun t1/0
    }.

t1() ->
    Map = fun({K, V}) -> {K, <<V/binary,"content">>} end,

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


