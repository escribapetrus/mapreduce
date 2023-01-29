%% Reducer is a gen_server that processes a list of keys previously processed by reducer,
%% fetching files by keys (filenames) in the file system (fs) 
%% and applying the Reduce Function
%%
%% The Reduce Function has the following signature: 
%% ({K1, V1} -> {K2, V2} | [{K2, V2}]
%% where K1 = V1 = K2 = V2 = binary()

%% where V1 is a file that collects all values for a given key,
%% and V2 is the final result for a given key.
%%
%% every key spawns a process that concurrently reduces the data.


-module(reducer).
-behaviour(gen_server).

-export([start_link/0, put_function/1, put_keys/1, run/0]).
-export([init/1, terminate/2, handle_cast/2, handle_call/3]).

-record(reducer_data, {function = undefined,
                       input = [],
                       completed = [],
                       failed = []}).

%% API
start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

put_function(F) -> gen_server:cast(?MODULE, {put_function, F}).

put_keys(Keys) -> gen_server:cast(?MODULE, {put_keys, Keys}).

put_completed(Key) -> gen_server:cast(?MODULE, {put_completed, Key}).

put_failed(Key) -> gen_server:cast(?MODULE, {put_failed, Key}).

run() -> gen_server:cast(?MODULE, run).

%% Private functions
emit(KVs) when is_list(KVs) ->
    lists:map(fun({K,V}) -> emit({K, V}) end, KVs);
emit({Key, Val}) ->
    fs:write({Key, Val}, result),
    {Key, Val}.

spawn_reducers(F, Inputs) ->
    lists:foreach(fun(K) -> 
                          spawn(fun() -> 
                                        try  
                                            {ok, FileData} = fs:read(K, reduce),
                                            emit(F({K, FileData}))
                                        of
                                            _ -> 
                                                put_completed(K)
                                        catch
                                            _:_ ->
                                                put_failed(K)
                                        end

                                end)
                  end,
                  Inputs).    

%% Callbacks
init([]) ->
    {ok, #reducer_data{}}.

handle_call(_, _, Data) ->
    {reply, Data, Data}.

handle_cast({put_function, F}, Data) ->
    {noreply, Data#reducer_data{function=F}};

handle_cast({put_keys, Keys}, Data) when is_list(Keys) ->
    {noreply, Data#reducer_data{input = Keys}};
handle_cast({put_keys, Key}, Data) ->
    {noreply, Data#reducer_data{input = [Key | Data#reducer_data.input]}};

handle_cast(run, #reducer_data{input = []} = Data) ->
    io:format("Error: input is empty. Please provide keys to be mapped~n", []),
    {noreply, Data};
handle_cast(run, #reducer_data{function = undefined} = Data) ->
    io:format("Error: function is nil. Please provide a reduce function"),
    {noreply, Data};
handle_cast(run, #reducer_data{function = Reduce, input = Inputs} = Data) ->
    spawn_reducers(Reduce, Inputs),
    {noreply, Data};

handle_cast({put_completed, Key}, #reducer_data{input = Input} = Data) ->
    NewData = Data#reducer_data{completed = [Key | Data#reducer_data.completed]},
    Processed = lists:flatten([NewData#reducer_data.completed, NewData#reducer_data.failed]),
    if length(Input) == length(Processed)  ->
            io:format("Finished processing.~n", []),
            {noreply, NewData};
       true ->
            io:format("Received new key as completed.~n", []),
            {noreply, NewData}
    end;

handle_cast({put_failed, Key}, #reducer_data{input = Input} = Data) ->
    NewData = Data#reducer_data{failed = [Key | Data#reducer_data.failed]},
    Processed = lists:flatten([NewData#reducer_data.completed, NewData#reducer_data.failed]),
    if length(Input) == length(Processed)  ->
            io:format("Finished processing.~n", []),
            {noreply, NewData};
       true ->
            io:format("Received new key as failed.~n", []),
            {noreply, NewData}
    end.

terminate(normal, _State) -> ok.
