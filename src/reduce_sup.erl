-module(reduce_sup).
-behaviour(supervisor).

-export([start_link/1, count_children/0, list_pids/0]).
-export([init/1]).

start_link(N) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, N).

count_children() ->
    {_, Count} = lists:keyfind(active, 1, supervisor:count_children(?MODULE)),
    Count.

list_pids() ->
    lists:map(fun({_, Pid, _, _}) -> Pid end, supervisor:which_children(?MODULE)).

init(N) ->
    SupFlags = #{strategy => one_for_one},
    ChildSpecs = lists:map(fun(Id) -> child_specs(Id) end,
                           lists:seq(0, N - 1)),
    {ok, {SupFlags, ChildSpecs}}.

child_specs(Id) ->
    #{id => Id,
      start => {mr_worker, start_link, [reduce]},
      restart => permanent,
      shutdown => brutal_kill,
      type => worker,
      modules => [mr_worker]}.


