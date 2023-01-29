%% Mapper is a gen_server that processes a list of keys,
%% fetching files by keys (filenames) in the file system (fs) 
%% and applying the Map function.
%%
%% The Map function has the following signature:
%% ({K :: binary(), V :: binary()}) -> {Key :: binary(), V :: binary()}
%%
%% every key spawns a process concurrently maps the data.

-module(mapper).
-behaviour(gen_server).

-export([start_link/0, put_function/1, put_keys/1, run/0]).
-export([init/1, terminate/2, handle_cast/2, handle_call/3]).


-record(mapper, {function = undefined, 
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
    fs:write({Key, Val}, reduce),
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
    {ok, #mapper{}}.

handle_call(_, _, Data) ->
    {reply, Data, Data}.

handle_cast({put_function, F}, Data) ->
    {noreply, Data#mapper{function=F}};

handle_cast({put_keys, [H|_] = Keys}, Data) when is_list(H) ->
    {noreply, Data#mapper{input = Keys}};
handle_cast({put_keys, Key}, Data) ->
    {noreply, Data#mapper{input = [Key | Data#mapper.input]}};

handle_cast(run, #mapper{input = []} = Data) ->
    io:format("Error: input is empty. Please provide keys to be mapped~n", []),
    {noreply, Data};
handle_cast(run, #mapper{function = undefined} = Data) ->
    io:format("Error: function is nil. Please provide a map function"),
    {noreply, Data};
handle_cast(run, #mapper{function = Map, input = Inputs} = Data) ->
    spawn_mappers(Map, Inputs), 
    {noreply, Data};

handle_cast({put_completed, Key}, #mapper{input = Input} = Data) ->
    NewData = 
        {mapper, _, _, Completed, Failed} = 
        append_data(completed, Key, Data),

    if length(Completed) + length(Failed) == length(Input) ->
            io:format("Finished processing.~n", []),
            reducer:run(),
            {noreply, NewData};
       true ->
            io:format("Mapper: Received new key as completed.~n", []),
            {noreply, NewData}
    end;

handle_cast({put_failed, Key}, #mapper{input = Input} = Data) ->
    NewData = 
        {mapper, _, _, Completed, Failed} = 
        append_data(failed, Key, Data),

    if length(Completed) + length(Failed) == length(Input) ->
            io:format("Finished processing.~n", []),
            sys:get_status(reducer),
            reducer:run(),
            {noreply, NewData};
       true ->
            io:format("Mapper: received new key as failed.~n", []),
            {noreply, NewData}
    end.

terminate(normal, _State) -> ok.

append_data(completed, Key, #mapper{completed = Completed} = Data) ->
    Data#mapper{completed = [Key | Completed]};
append_data(failed, Key, #mapper{failed = Failed} = Data) ->
    Data#mapper{failed = [Key | Failed]}.
