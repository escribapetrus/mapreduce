%%%-------------------------------------------------------------------
%% @doc mapreduce public API
%% @end
%%%-------------------------------------------------------------------

-module(mapreduce_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    Mappers =  proplists:get_value(
                 num_workers, 
                 application:get_env(mapreduce, map_sup, 1)),

    Reducers= proplists:get_value(
                num_workers, 
                application:get_env(mapreduce, reduce_sup, 1)),

    mapreduce_sup:start_link(Mappers, Reducers).

stop(_State) ->
    ok.

%% internal functions
