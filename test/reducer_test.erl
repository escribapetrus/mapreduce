-module(reducer_test).
-include_lib("eunit/include/eunit.hrl").

setup() -> 
    mapreduce_sup:start_link(),
    ok.

reduce_test_() ->
    {setup,
     fun setup/0,
     [fun reduce_returns_single_value/0,
      fun reduce_returns_multiple_values/0]
    }.

reduce_returns_single_value() ->
    Inputs = ["grass", "poison"],
    Reduce = fun({K, V}) ->
                     Lines = lists:filter(fun(X) -> jsx:is_json(X) end, binary:split(V, <<"\n">>, [global])),
                     ParsedLines = lists:map(fun(X) -> jsx:decode(X) end, Lines),
                     {_, Res} = lists:foldr(fun(X, {AccWeight, Acc}) -> 
                                                    XWeight = maps:get(<<"weight">>, X),
                                                    if 
                                                        XWeight > AccWeight -> {XWeight, X};
                                                        true -> {AccWeight, Acc}
                                                    end
                                            end, {0, nil}, ParsedLines),
                     {K, jsx:encode(Res)}
             end,

    reducer:put_keys(Inputs),
    reducer:put_function(Reduce),
    reducer:run(),

    timer:sleep(1000),

    {reducer, Reduce, SourceInputs, Completed, _Failed} = sys:get_state(reducer),
    true = lists:all(fun(X) -> lists:member(X, Completed) end, SourceInputs).

reduce_returns_multiple_values() ->
    Inputs = ["grass", "poison"],
    Reduce = fun({K, V}) ->
                     Lines = lists:filter(fun(X) -> jsx:is_json(X) end, binary:split(V, <<"\n">>, [global])),
                     ParsedLines = lists:map(fun(X) -> jsx:decode(X) end, Lines),
                     {_, Res} = lists:foldr(fun(X, {AccWeight, Acc}) -> 
                                                    XWeight = maps:get(<<"weight">>, X),
                                                    if 
                                                        XWeight > AccWeight -> {XWeight, X};
                                                        true -> {AccWeight, Acc}
                                                    end
                                            end, {0, nil}, ParsedLines),
                     [{K, jsx:encode(Res)}, {K ++ "_otherfile", jsx:encode(Res)}]
             end,

    reducer:put_keys(Inputs),
    reducer:put_function(Reduce),
    reducer:run(),

    timer:sleep(1000),

    {reducer, Reduce, SourceInputs, Completed, _Failed} = sys:get_state(reducer),
    true = lists:all(fun(X) -> lists:member(X, Completed) end, SourceInputs).
