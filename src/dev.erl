-module(dev).
-export([mapreduce/0, mapreduce/1]).

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

    mr:process(Inputs).

mapreduce(seq) ->
    MapKeys = lists:map(fun(N) -> "pokemon_" ++ integer_to_list(N) end, lists:seq(1,1200)),
    lists:foreach(fun(K) -> 
                          try 
                              {ok, FileData} = fs:read(K, map),
                              Pokemon = jsx:decode(FileData),
                              Types = maps:get(<<"types">>, Pokemon),
                              Data = #{<<"name">> => maps:get(<<"name">>, Pokemon),
                                       <<"height">> => maps:get(<<"height">>, Pokemon),
                                       <<"weight">> => maps:get(<<"weight">>, Pokemon)},
                              lists:map(fun(T) ->
                                                TypeName = maps:get(<<"name">>, maps:get(<<"type">>, T)),
                                                {binary_to_list(TypeName), jsx:encode(Data)}
                                        end, Types)  
                          of
                              KVs when is_list(KVs) ->
                                  lists:foreach(fun({K1,V1}) -> fs:write({K1, V1}, reduce) end, KVs);

                              KV ->
                                  fs:write(KV, reduce)
                          catch
                              _:_ -> 
                                  io:format("[Map] failed to process key ~p.~n", [K]),
                                  error
                          end
                  end, MapKeys),
    {ok, ReduceKeys} = file:list_dir("fs_reduce"),
    lists:foreach(fun(K) -> 
                          try 
                              {ok, FileData} = fs:read(K, reduce),
                              Lines = lists:filter(fun(X) -> jsx:is_json(X) end, binary:split(FileData, <<"\n">>, [global])),
                              ParsedLines = lists:map(fun(X) -> jsx:decode(X) end, Lines),
                              {_, Res} = lists:foldr(fun(X, {AccWeight, Acc}) -> 
                                                             XWeight = maps:get(<<"weight">>, X),
                                                             if 
                                                                 XWeight > AccWeight -> {XWeight, X};
                                                                 true -> {AccWeight, Acc}
                                                             end
                                                     end, {0, nil}, ParsedLines),
                              {K, jsx:encode(Res)}
                          of
                              KVs when is_list(KVs) ->
                                  lists:foreach(fun(KV) -> fs:write(KV, reduce) end, KVs);
                              KV ->
                                  fs:write(KV, reduce)
                          catch
                              _:_ -> 
                                  io:format("[Reduce] failed to process key ~p.~n", [K]),
                                  error
                          end
                  end, ReduceKeys).

