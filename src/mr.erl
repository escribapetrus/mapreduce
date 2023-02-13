-module(mr).
-export([define_map/1, define_reduce/1, run/1]).


define_map(F) -> 
    lists:foreach(fun(Pid) -> mr_worker:put_function(Pid, F) end, 
                  map_sup:list_pids()).

define_reduce(F) ->
    lists:foreach(fun(Pid) -> mr_worker:put_function(Pid, F) end, 
                  reduce_sup:list_pids()).

run(Keys) ->
    put_keys(Keys, map),
    mr_manager:run(map).

put_keys([H | _] = Keys, Type) when is_list(H) ->
    lists:foreach(fun(K) -> mr_manager:put_key(K, Type) end, Keys).
