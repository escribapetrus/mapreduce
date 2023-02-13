-module(mr).
-export([define_map/1, define_reduce/1, run/1]).


define_map(F) -> 
    lists:foreach(fun(Pid) -> mr_worker:put_function(Pid, F) end, 
                  map_sup:list_pids()).

define_reduce(F) ->
    lists:foreach(fun(Pid) -> mr_worker:put_function(Pid, F) end, 
                  reduce_sup:list_pids()).

run(Keys) ->
    mr_manager:put_keys(Keys, map),
    timer:sleep(5),
    mr_manager:run(map).
