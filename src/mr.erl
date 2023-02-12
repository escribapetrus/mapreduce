-module(mr).
-export([define_map/1, define_reduce/1, put_keys/2, run/1]).

-define(CRYPTO_ALG, sha256). 

define_map(F) -> 
    lists:foreach(fun(Pid) -> mr_worker:put_function(Pid, F) end, 
                  map_sup:list_pids()).

define_reduce(F) ->
    lists:foreach(fun(Pid) -> mr_worker:put_function(Pid, F) end, 
                  reduce_sup:list_pids()).

put_keys([H | _] = Keys, map) when is_list(H) ->
    lists:foreach(fun(K) -> put_keys(K, map) end, Keys);
put_keys(Key, map) ->   
    MapCount = map_sup:count_children(),
    Target = lists:nth(target(Key, MapCount) + 1, map_sup:list_pids()),
    mr_worker:put_key(Target, Key);     

put_keys([H |_ ] = Keys, reduce) when is_list(H) ->
    lists:foreach(fun(K) -> put_keys(K, reduce) end, Keys);
put_keys(Key, reduce) ->
    ReduceCount = reduce_sup:count_children(),
    Target = lists:nth(target(Key, ReduceCount) + 1, reduce_sup:list_pids()),
    mr_worker:put_key(Target, Key).  

-spec target(Key :: string() | binary(), N :: integer()) -> integer().
target(Key, N) ->
    Int = crypto:bytes_to_integer(crypto:hash(?CRYPTO_ALG, Key)),
    Int rem N.

run(map) ->
    lists:foreach(fun(Pid) -> mr_worker:run(Pid) end, 
                  map_sup:list_pids());    
run(reduce) ->
    lists:foreach(fun(Pid) -> mr_worker:run(Pid) end, 
                  reduce_sup:list_pids());
run(result) -> 
    ok;
run(Keys) ->
    put_keys(Keys, map),
    run(map).
