%% Mapper is a gen_server that processes a list of keys,
%% fetching files by keys (filenames) in the file system (fs) 
%% and applying the Map function.
%%
%% The Map function has the following signature:
%% ({K :: binary(), V :: binary()}) -> {Key :: binary(), V :: binary()}
%%
%% every key spawns a process concurrently maps the data.

%% TODO: 
%% Two options:
%% - emit appends data to a file in the reducer file system,
%%   then processed when the mapper finishes processing (receives all keys as completed/failed)
%% - emit passes the processed KV to the reducer in memory

-module(mapper).
-behaviour(gen_server).

-export([start_link/0, put_function/1, put_keys/1, run/0]).
-export([init/1, terminate/2, handle_cast/2, handle_call/3]).


-record(mapper_data, {function = undefined, 
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
    lists:map(fun({K,V}) -> emit({K,V}) end, KVs);
emit({Key, Val}) ->
    fs:append({Key, Val}, reduce),
    reducer:put_keys(Key),
    {Key, Val}.

spawn_mappers(F, Inputs) ->
    lists:foreach(fun(K) -> 
                          spawn(fun() -> 
                                        try  
                                            {ok, FileData} = fs:read(K, map),
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
    {ok, #mapper_data{}}.

handle_call(_, _, Data) ->
    {reply, Data, Data}.

handle_cast({put_function, F}, Data) ->
    {noreply, Data#mapper_data{function=F}};

handle_cast({put_keys, Keys}, Data) when is_list(Keys) ->
    {noreply, Data#mapper_data{input = Keys}};
handle_cast({put_keys, Key}, Data) ->
    {noreply, Data#mapper_data{input = [Key | Data#mapper_data.input]}};

handle_cast(run, #mapper_data{input = []} = Data) ->
    io:format("Error: input is empty. Please provide keys to be mapped~n", []),
    {noreply, Data};
handle_cast(run, #mapper_data{function = undefined} = Data) ->
    io:format("Error: function is nil. Please provide a map function"),
    {noreply, Data};
handle_cast(run, #mapper_data{function = Map, input = Inputs} = Data) ->
    spawn_mappers(Map, Inputs), 
    {noreply, Data};

handle_cast({put_completed, Key}, #mapper_data{input = Input} = Data) ->
    NewData = Data#mapper_data{completed = [Key | Data#mapper_data.completed]},
    Processed = lists:flatten([NewData#mapper_data.completed, NewData#mapper_data.failed]),
    if length(Input) == length(Processed)  ->
            io:format("Finished processing.~n", []),
            {noreply, NewData};
       true ->
            io:format("Received new key as completed.~n", []),
            {noreply, NewData}
    end;

handle_cast({put_failed, Key}, #mapper_data{input = Input} = Data) ->
    NewData = Data#mapper_data{failed = [Key | Data#mapper_data.failed]},
    Processed = lists:flatten([NewData#mapper_data.completed, NewData#mapper_data.failed]),
    if length(Input) == length(Processed)  ->
            io:format("Finished processing.~n", []),
            reducer:run(),
            {noreply, NewData};
       true ->
            io:format("Received new key as failed.~n", []),
            {noreply, NewData}
    end.

terminate(normal, _State) -> ok.
