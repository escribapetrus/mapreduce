# mapreduce

**UNDER CONSTRUCTION**

MapReduce is a massively parallel program for data analysis.
The program is structured as a higher-order function, inspired by Lisp and functional programming.

The purpose of MapReduce is to abstract the parallel compuration architecture, leaving the programmer
with the simple task of defining two functions that define how the data is supposed to be computed.

As stated by the original paper on the subject:
>MapReduce is a programming model and an associated implementation for processing and generating
>large data sets. Users specify a /map/ function that processes a key-value pair to generate
>a set of intermediate key/value pair, and a /reduce/ function that merges all intermediate values
>associated with the same intermediate key.
>
>We designed a new abstraction that allows us to express the simple computations we were trying to perform
>but hides the messy details of parallelization, fault tolerance, data distribution and load balancing in a library.
>-- Jeffrey Dean and Sanjay Ghemawat, Google Inc.

## Use cases
You can use MapReduce, for example, to:
- Count word occurences in a series of documents
- Group products by section, and get the best seller
- Rank paintings by author, and get the best ranked

## How to use
First, compile the project. You can run it in the Rebar3 shell.

```sh
    $ rebar3 compile
	$ rebar3 shell
```

In order to use MapReduce:
1. Store the data set in the **Mapper File System** folder `fs_map/`, where every file is a single data item
   in a format you can compute (raw, json, csv...);
2. Define a **Map function**, using `mr:define_map/1`. 
   Map computes the data item and adds the result to the reducer.
   Mapped data items are grouped by key in the **Reducer File System** `fs/reduce`.
   Map has the following signature: 
   `(K1 :: binary(), V1 :: binary()) -> {K2 :: binary(), V2 :: binary()} | list({K2, V2})`;
3. Define a **Reduce function**, using `mr:define_reduce/1`. 
   Result computes the data items grouped in a key into a single result.
   The result is stored by key in the **Result File System** `fs/result`. 
   Map has the following signature: 
   `(K1 :: binary(), V1 :: binary()) -> {K2 :: binary(), V2 :: binary()} | list({K2, V2})`;
4. Process the list of keys you want by passing it to `mr:process/1`.


## Architecture
Concurrent programs in Erlang run in execution units called processes. For the purposes of our implementation of MapReduce,
all nodes (e.g. mappers, reducers) are processes.

Erlang concurrent programs are structured in *supervision trees*. This is a hierarchical structure where certain parent processes,
called *supervisors*, spawn, monitor and restart child processes (which can be either supervisors or workers).

Our implementation allows spawning N map and reduce servers, and spawns concurrent processes for every key 
to be mapped or reduced.

## Example
In the Rebar3 shell, we run the following program to get the heaviest pokemon for each type (e.g. grass, fire, rock).

```erlang
    Inputs = lists:map(fun(N) -> "pokemon_" ++ integer_to_list(N) end, lists:seq(1,1200)),
    mr:define_map(fun({_K, V}) -> 
                  Pokemon = jsx:decode(V),
                  Types = maps:get(<<"types">>, Pokemon),
                  Data = #{<<"name">> => maps:get(<<"name">>, Pokemon),
                           <<"height">> => maps:get(<<"height">>, Pokemon),
                           <<"weight">> => maps:get(<<"weight">>, Pokemon)},
                  lists:map(fun(T) ->
                                    TypeName = maps:get(<<"name">>, maps:get(<<"type">>, T)),
                                    {binary_to_list(TypeName), jsx:encode(Data)}
                            end, Types)
          end),
    mr:define_reduce(fun({K, V}) ->
                     Lines = lists:filter(fun(X) -> jsx:is_json(X) end, binary:split(V, <<"\n">>, [global])),
                     ParsedLines = lists:map(fun(X) -> jsx:decode(X) end, Lines),
                     {_, Res} = lists:foldr(fun(X, {AccWeight, Acc}) -> 
                                                    XWeight = maps:get(<<"weight">>, X),
                                                    if 
                                                        XWeight > AccWeight -> {XWeight, X};
                                                        true -> {AccWeight, Acc}
                                                    end
                                            end, {0, nil}, ParsedLines),
                     {K, jsx:encode(Res)}
             end),

    mr:run(Inputs).
```
