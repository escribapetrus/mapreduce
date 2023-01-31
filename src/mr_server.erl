-module(mr_server).
-behaviour(gen_server).

-export([start_link/1, put_function/2, put_key/2, run/1]).
-export([init/1, terminate/2, handle_cast/2, handle_call/3]).


-record(mr_server, {type,
		    function = undefined, 
		    input = [], 
		    completed = [], 
		    failed = []}).

%% API
start_link(Type) -> gen_server:start_link(?MODULE, Type, []).

put_function(Pid, F) -> gen_server:cast(Pid, {put_function, F}).

put_key(Pid, Key) -> gen_server:cast(Pid, {put_key, Key}).

put_completed(Pid, Key) -> gen_server:cast(Pid, {put_completed, Key}).

put_failed(Pid, Key) -> gen_server:cast(Pid, {put_failed, Key}).

run(Pid) -> gen_server:cast(Pid, run).

%% Callbacks
init(Type) ->
    {ok, #mr_server{type = Type}}.

handle_call(_, _, Data) ->
    {reply, Data, Data}.

handle_cast({put_function, F}, Data) ->
    {noreply, Data#mr_server{function=F}};

handle_cast({put_key, Key}, Data) ->
    {noreply, Data#mr_server{input = [Key | Data#mr_server.input]}};

handle_cast(run, #mr_server{input = []} = Data) ->
    io:format("Error: input is empty. Please provide keys to be mapped~n", []),
    {noreply, Data};
handle_cast(run, #mr_server{function = undefined} = Data) ->
    io:format("Error: function is nil. Please provide a map function"),
    {noreply, Data};
handle_cast(run, #mr_server{function = F, input = Input, type = Type} = Data) ->
    spawn_mr(F, Input, Type, self()),
    {noreply, Data};

handle_cast({put_completed, Key}, #mr_server{input = Input, type = map} = Data) ->
    NewData = 
        {mr_server, _, _, _, Completed, Failed} = 
        append_data(completed, Key, Data),

    if length(Completed) + length(Failed) == length(Input) ->
            io:format("Map: finished processing.~n", []),
	    mr:put_keys(Completed, reduce),
	    timer:sleep(10),
            mr:run(reduce),
            {noreply, NewData};
       true ->
            io:format("Map: received new key as completed.~n", []),
            {noreply, NewData}
    end;

handle_cast({put_failed, Key}, #mr_server{input = Input, type = map} = Data) ->
    NewData = 
        {mr_server, _, _, _, Completed, Failed} = 
        append_data(failed, Key, Data),

    if length(Completed) + length(Failed) == length(Input) ->
            io:format("Map: finished processing.~n", []),
	    mr:put_keys(Completed, reduce),
	    timer:sleep(10),
            mr:run(reduce),
            {noreply, NewData};
       true ->
            io:format("Map: received new key as failed.~n", []),
            {noreply, NewData}
    end;
handle_cast({put_completed, Key}, #mr_server{input = Input, type = reduce} = Data) ->
    NewData = 
        {mr_server, _, _, _, Completed, Failed} = 
        append_data(completed, Key, Data),

    if length(Completed) + length(Failed) == length(Input) ->
            io:format("Reduce: finished processing.~n", []),
            {noreply, NewData};
       true ->
            io:format("Reduce: Received new key as completed.~n", []),
            {noreply, NewData}
    end;

handle_cast({put_failed, Key}, #mr_server{input = Input, type = reduce} = Data) ->
    NewData = 
        {mr_server, _, _, _, Completed, Failed} = 
        append_data(failed, Key, Data),

    if length(Completed) + length(Failed) == length(Input) ->
            io:format("Reduce: finished processing.~n", []),
            {noreply, NewData};
       true ->
            io:format("Reduce: received new key as failed.~n", []),
            {noreply, NewData}
    end.

terminate(normal, _State) -> ok.

%% Private functions
emit(KVs, Type) when is_list(KVs) ->
    lists:map(fun(KV) -> emit(KV, Type) end, KVs);
emit({Key, Val}, map) ->
    fs:write({Key, Val}, map),
    {Key, Val};
emit({Key, Val}, reduce) ->
    fs:write({Key, Val}, reduce),
    {Key, Val}.

spawn_mr(F, Inputs, Type, Parent) ->
    lists:foreach(fun(K) -> 
                          spawn(fun() -> 
                                        try  
                                            {ok, FileData} = fs:read(K, Type),
                                            emit(F({K, FileData}), Type)
                                        of
                                            _ ->
                                                put_completed(Parent, K)
                                        catch
                                            _:_ -> 
                                                put_failed(Parent, K)
                                        end

                                end)
                  end,
                  Inputs).


append_data(completed, Key, #mr_server{completed = Completed} = Data) ->
    Data#mr_server{completed = [Key | Completed]};
append_data(failed, Key, #mr_server{failed = Failed} = Data) ->
    Data#mr_server{failed = [Key | Failed]}.
