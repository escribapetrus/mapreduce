-module(mr).
-export([define_map/1, define_reduce/1, process/1, put_keys/2, run/1]).

-define(CRYPTO_ALG, sha256). 

define_map(F) -> 
    lists:foreach(fun(Pid) -> mr_server:put_function(Pid, F) end, 
		  map_sup:list_pids()).


define_reduce(F) ->
    lists:foreach(fun(Pid) -> mr_server:put_function(Pid, F) end, 
		  reduce_sup:list_pids()).

put_keys(Keys, map) ->
    lists:foreach(fun(K) ->
			  MapCount = map_sup:count_children(),
			  Target = map_sup:get_pid(target(K, MapCount)),
			  mr_server:put_key(Target, K) end, Keys);

put_keys(Keys, reduce) ->
    lists:foreach(fun(K) ->
			  ReduceCount = reduce_sup:count_children(),
			  Target = reduce_sup:get_pid(target(K, ReduceCount)),
			  mr_server:put_key(Target, K) end, Keys).

process(Keys) ->
    put_keys(Keys, map),
    timer:sleep(10),
    run(map).
    
-spec target(Key :: string() | binary(), N :: integer()) -> integer().
target(Key, N) ->
    Int = crypto:bytes_to_integer(crypto:hash(?CRYPTO_ALG, Key)),
    Int rem N.


run(map) ->
    lists:foreach(fun(Pid) -> mr_server:run(Pid) end, 
		  map_sup:list_pids());    
run(reduce) ->
    lists:foreach(fun(Pid) -> mr_server:run(Pid) end, 
		  reduce_sup:list_pids()).  
