-module(mapper_sup).
-behaviour(supervisor).

-export([start_link/2, add_mapper/0, count_children/0, list_pids/0, get_pid/1]).
-export([init/1]).

start_link(Func, N) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, {Func, N}).

count_children() ->
    {_, Count} = lists:keyfind(active, 1, supervisor:count_children(?MODULE)),
    Count.

list_pids() ->
    lists:map(fun({_, Pid, _, _}) -> Pid end, supervisor:which_children(?MODULE)).

get_pid(Id) ->
    {_, Pid, _, _} = lists:keyfind(Id, 1, supervisor:which_children(?MODULE)),
    Pid.

get_function() ->
    {ok, Specs} = supervisor:get_childspec(?MODULE, 1),
    {_, _, [Func|_]} = maps:get(start, Specs),
    Func.

init({Func, N}) ->
    %% Create ETS Table to keep store of mappers
    %% ets:new(mapper_workers, [set, named_table, public]),

    SupFlags = #{strategy => one_for_one, intensity => 1, period => 5},
    ChildSpecs = lists:map(fun(Id) -> child_specs(Func, Id) end,
                           lists:seq(0, N-1)),
    {ok, {SupFlags, ChildSpecs}}.

child_specs(Func, Id) ->
    #{id => Id,
      start => {mapper, start_link, [Func]},
      restart => permanent,
      shutdown => brutal_kill,
      type => worker,
      modules => [mapper]}.

%% TODO: Add new mappers
%% Count mappers (supervisor children) and 
%% Create a new one with the same function.
add_mapper() ->
    MappersCount = count_children(),
    Func = get_function(),
    supervisor:start_child(?MODULE, child_specs(Func, MappersCount + 1)).
