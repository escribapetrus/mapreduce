-module(mapreduce_sup).

-behaviour(supervisor).

-export([start_link/2]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link(NumMappers, NumReducers) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, {NumMappers, NumReducers}).

init({NumMappers, NumReducers}) ->
    SupFlags = #{strategy => one_for_all,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [
                  #{id => map_sup, start => {map_sup, start_link, [NumMappers]}},      
                  #{id => reduce_sup, start => {reduce_sup, start_link, [NumReducers]}}     
                 ],
    {ok, {SupFlags, ChildSpecs}}.


