%%%-------------------------------------------------------------------
%% @doc mapreduce public API
%% @end
%%%-------------------------------------------------------------------

-module(mapreduce_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    NumMappers =  proplists:get_value(
	   num_workers, 
	   application:get_env(mapreduce, map_sup, 1)),
    
    NumReducers= proplists:get_value(
	    num_workers, 
	    application:get_env(mapreduce, reduce_sup, 1)),

    mapreduce_sup:start_link(NumMappers, NumReducers).

stop(_State) ->
    ok.

%% internal functions
