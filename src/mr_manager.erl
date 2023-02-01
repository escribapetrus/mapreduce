-module(mr_manager).
-behaviour(gen_server).

-export([start_link/2, put_completed_worker/1]).
-export([init/1, terminate/2, handle_cast/2, handle_call/3, handle_info/2]).


-record(mr_manager, {mapped = 0,
                     mappers,
                     reduced = 0,
                     reducers
                    }).

%% API
start_link(NumMap, NumReduce) -> gen_server:start_link({local, ?MODULE}, ?MODULE, {NumMap, NumReduce}, []).

put_completed_worker(Type) -> gen_server:cast(?MODULE, {put_completed_worker, Type}).

%% Callbacks
init({NumMap, NumReduce}) ->
    {ok, #mr_manager{mappers = NumMap, reducers = NumReduce}}.

handle_call(_, _, Data) ->
    {reply, Data, Data}.

handle_cast({put_completed_worker, map}, #mr_manager{mapped = N, mappers = T } = Data) ->
    Mapped = N + 1,

    if Mapped == T ->
            mr:run(reduce),
            {noreply, Data#mr_manager{mapped = Mapped}};
       true -> 
            {noreply, Data#mr_manager{mapped = Mapped}}
    end;
handle_cast({put_completed_worker, reduce}, #mr_manager{reduced = N, reducers = T } = Data) ->
    Reduced = N + 1,

    if Reduced == T ->
            mr:run(result),
            {noreply, Data#mr_manager{reduced = Reduced}};
       true -> 
            {noreply, Data#mr_manager{reduced = Reduced}}
    end.

handle_info({'EXIT', _Pid, normal}, State) ->
    {noreply, State}.

terminate(normal, _State) -> ok.
