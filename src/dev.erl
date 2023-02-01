-module(dev).
-export([mapreduce/0]).

mapreduce() ->
    Inputs = lists:map(fun(N) -> "pokemon_" ++ integer_to_list(N) end, lists:seq(1,1300)),
    mr:define_map(fun({_K, V}) -> 
                          Pokemon = jsx:decode(V),
                          Types = maps:get(<<"types">>, Pokemon),
                          Data = #{<<"name">> => maps:get(<<"name">>, Pokemon),
                                   <<"height">> => maps:get(<<"height">>, Pokemon),
                                   <<"weight">> => maps:get(<<"weight">>, Pokemon)},
                          lists:map(fun(T) ->
                                            TypeName = maps:get(<<"name">>, maps:get(<<"type">>, T)),
                                            {binary_to_list(TypeName), jsx:encode(Data)}
                                    end, Types)
                  end),
    mr:define_reduce(fun({K, V}) ->
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
                     end),
    mr:run(Inputs).

