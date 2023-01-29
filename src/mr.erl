-module(mr).
-export([define_map/1, define_reduce/1, process/1]).

define_map(F) -> mapper:put_function(F).

define_reduce(F) -> reducer:put_function(F).

process(Keys) ->
    mapper:put_keys(Keys),
    timer:sleep(10),
    mapper:run().

