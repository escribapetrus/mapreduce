-module(mapreduce_sup).

-behaviour(supervisor).

-export([start_link/2]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link(Mappers, Reducers) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, {Mappers, Reducers}).

init({Mappers, Reducers}) ->
    SupFlags = #{strategy => one_for_one,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [
                  #{id => map_sup, 
                    type => supervisor,
                    restart => permanent,
                    start => {map_sup, start_link, [Mappers]}},
                  #{id => reduce_sup, 
                    type => supervisor,
                    restart => permanent,
                    start => {reduce_sup, start_link, [Reducers]}},
                  #{id => mr_manager,
                    type => worker,
                    restart => permanent,
                    start => {mr_manager, start_link, [Mappers, Reducers]}}
                 ],
    {ok, {SupFlags, ChildSpecs}}.


