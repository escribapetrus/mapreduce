-module(reducer_test).
-include_lib("eunit/include/eunit.hrl").

setup() -> ok.

reduce_test_() ->
    {setup,
     fun setup/0,
     fun t1/0
    }.

t1() ->
    Reduce = fun({K, V}) ->
                     Lines = binary:split(V, <<"\n">>, [global]),
                     [H|ParsedLines] = lists:map(fun(X) -> jsx:decode(X) end, Lines),
                     Res = lists:foldr(fun(X, Acc) -> 
                                               XWeight = maps:get(<<"weight">>, X),
                                               AccWeight = maps:get(<<"weight">>, Acc), 
                                               if 
                                                   XWeight > AccWeight -> X;
                                                   true -> Acc
                                               end
                                       end, H, ParsedLines),
                     {K, Res}
             end,

    {ok, Pid} = reducer:start_link(),
    reducer:put_keys([
                      "mammals",
                      "plants",
                      "reptiles"]),
    reducer:put_function(Reduce),
    reducer:run(),

    timer:sleep(1000),

    ?assertEqual(
       sys:get_state(Pid), 
       {reducer_data,Reduce,
        ["mammals","plants","reptiles"],
        ["reptiles","plants","mammals"],
        []}).

