%% Mapper is a light genserver that processes an input file, 
%% producing a computed intermediate key and value pair
%% that is passed to the reducer
%%
%% Receives a Map function of type:
%% ({K :: binary(), V :: binary()}) -> {Key :: binary(), V :: binary()}


%% Idea:
%% There is only one mapper: a genserver (or statem) that is configured with a map function,
%% and receives a process request with a list of inputs (keys)

%% every key spawns a map function, which returns a result to the mapper. 
%% When all results are done, and all reduce files are saved,
%%  we can call the reduce server.

-module(mapper).
-behaviour(gen_server).

-export([start_link/0, put_function/1, put_keys/1, run/0]).
-export([callback_mode/0, init/1, terminate/2, handle_cast/2, handle_call/3]).


-record(mapper_data, {
                      function = undefined, 
                      input = [], 
                      completed = [], 
                      failed = []}).

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

put_function(F) -> gen_server:cast(?MODULE, {put_function, F}).

put_keys(Keys) -> gen_server:cast(?MODULE, {put_keys, Keys}).

put_completed(Key) -> gen_server:cast(?MODULE, {put_completed, Key}).

put_failed(Key) -> gen_server:cast(?MODULE, {put_failed, Key}).

run() -> gen_server:cast(?MODULE, run).

emit({Key, Val}) ->
    io:format("Emitted: KEY[~p], VAL[~p]~n", [Key, Val]), 
    {ok, {Key, Val}}.


%% Callbacks
callback_mode() ->
    state_functions.

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
    F = fun(K) -> 
                spawn(fun() -> 
                              try  
                                  {ok, FileData} = fs:read(K, map),
                                  {ok, _Emitted} = emit(Map({K, FileData}))
                              of
                                  _ -> 
                                      put_completed(K)
                              catch
                                  _:_ -> 
                                      put_failed(K)
                              end

                      end)
        end,
    lists:foreach(F, Inputs),
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
            {noreply, NewData};
       true ->
            io:format("Received new key as failed.~n", []),
            {noreply, NewData}
    end.

terminate(normal, _State) -> ok.
