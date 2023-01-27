%%%-------------------------------------------------------------------
%% @doc mapreduce public API
%% @end
%%%-------------------------------------------------------------------

-module(mapreduce_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    mapreduce_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
