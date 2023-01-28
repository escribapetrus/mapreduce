%% File System API
%% Exposes a collection of functions used to interact with 
%% the Reduce and Map file systems.

-module(fs).
-export([read/2, read/3, write/2, append/2]).

read(Filename, map) ->
    file:read_file("fs_map/" ++ Filename);
read(Filename, reduce) ->
    file:read_file("fs_reduce/" ++ Filename).

read(Filename, map, {as, json}) ->
    {ok, Data} = read(Filename, map),
    {ok, jsx:decode(Data)};
read(Filename, reduce, {as, json}) ->
    {ok, Data} = read(Filename, reduce),
    {ok, jsx:decode(Data)}.

append({Filename, Content}, reduce) ->
    file:write_file("fs_reduce/" ++ Filename, <<Content/binary, "\n">>, [append]).

write({Filename, Content}, map) ->
    file:write_file("fs_map/" ++ Filename, Content, []);
write({Filename, Content}, result) ->
    file:write_file("fs_result/" ++ Filename, Content, []).

