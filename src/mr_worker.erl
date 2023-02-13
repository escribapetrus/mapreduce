-module(mr_worker).
-behaviour(gen_server).

-export([start_link/1, put_function/2, put_key/2, run/1]).
-export([init/1, handle_cast/2, handle_call/3, handle_info/2]).


-record(mr_worker, {type,
                    function = undefined, 
                    input = [],
                    processed = 0
                   }).

%% API
start_link(Type) -> gen_server:start_link(?MODULE, Type, []).

put_function(Pid, F) -> gen_server:cast(Pid, {put_function, F}).

put_key(Pid, Key) -> gen_server:cast(Pid, {put_key, Key}).

run(Pid) -> gen_server:cast(Pid, run).

%% Callbacks
init(Type) ->
    process_flag(trap_exit, true),
    {ok, #mr_worker{type = Type}}.

handle_call(_, _, Data) ->
    {reply, Data, Data}.

handle_cast({put_function, F}, Data) ->
    {noreply, Data#mr_worker{function=F}};

handle_cast({put_key, Key}, Data) ->
    {noreply, Data#mr_worker{input = [Key | Data#mr_worker.input]}};

handle_cast(run, #mr_worker{input = [], type = Type} = Data) ->
    io:format("[~p] Error: input is empty. Please provide keys.~n", [Type]),
    {noreply, Data};
handle_cast(run, #mr_worker{function = undefined, type = Type} = Data) ->
    io:format("[~p] Error: function is nil. Please provide a function.", [Type]),
    {noreply, Data};
handle_cast(run, #mr_worker{function = F, input = Input, type = Type} = Data) ->
    spawn_mr(F, Input, Type),
    {noreply, Data}.

handle_info({'EXIT', _Pid, _}, #mr_worker{processed = N, input = Input, type = Type} = State) ->
    Processed = N + 1,

    if Processed == length(Input) ->
            mr_manager:put_completed_worker(Type),
            {noreply,  #mr_worker{type = Type}};
       true ->
            {noreply, State#mr_worker{processed = Processed}}
    end.

%% Private functions
emit(KVs, Type) when is_list(KVs) ->
    lists:map(fun(KV) -> emit(KV, Type) end, KVs);
emit({Key, Val}, map) ->
    fs:write({Key, Val}, reduce),
    mr_manager:put_keys(Key, reduce),
    {Key, Val};
emit({Key, Val}, reduce) ->
    fs:write({Key, Val}, result),
    {Key, Val}.

spawn_mr(F, Inputs, Type) ->
    lists:foreach(fun(K) -> 
                          spawn_link(fun() ->
                                             try
                                                 {ok, FileData} = fs:read(K, Type),
                                                 emit(F({K, FileData}), Type)
                                             of KV -> KV
                                             catch _:_ -> fail
                                             end
                                     end)
                  end,
                  Inputs).
