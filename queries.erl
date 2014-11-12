-module(queries).
-export([splitos/1,
        most_problems/1,
        use_features_by/2]).

splitos(Os)->
    os:cmd("riak start"),
    timer:sleep(3000),
    Pid = setup_data:connect(),
    {ok, Result} = riakc_pb_socket:get_index_eq(Pid, 
                              list_to_binary("Product"), 
                              {binary_index, "os"},
                              list_to_binary(Os)),  
   riakc_pb_socket:stop(Pid),
   {_,R,_,_} = Result,
   Number = size(list_to_tuple(R)),
   Res = [R,Number],
   io:format("~p", [Res]).

use_features_by(C,Value)->
    os:cmd("riak start"),
    timer:sleep(3000),
    Pid = setup_data:connect(),
    Query =  case C  of
        "version" ->
            {ok, Result} = riakc_pb_socket:get_index_eq(Pid,
                list_to_binary("Product"),
                {binary_index, "version"},
                list_to_binary(Value)),
            riakc_pb_socket:stop(Pid),
            {_,R,_,_} = Result,
            N = size(list_to_tuple(R)),
            Res = [R,N],
            Res;
        "day"->
            {ok, Result} = riakc_pb_socket:get_index_eq(Pid,
                list_to_binary("Product"),
                 {binary_index, "day"},
                 list_to_binary(Value)),
            riakc_pb_socket:stop(Pid),
            {_,R,_,_} = Result,
            N = size(list_to_tuple(R)),
            Res = [R,N],
            Res;
         "month"->
            {ok, Result} = riakc_pb_socket:get_index_eq(Pid,
                list_to_binary("Product"),
                 {binary_index, "month"},
                 list_to_binary(Value)),
            riakc_pb_socket:stop(Pid),
            {_,R,_,_} = Result,
            N = size(list_to_tuple(R)),
            Res = [R,N],
            Res;
          "all" ->
            {ok, Result} = riakc_pb_socket:get_index_eq(Pid,
                list_to_binary("Product"),
                {binary_index, "product"},
                list_to_binary("Younited")),
            riakc_pb_socket:stop(Pid),
            {_,R,_,_} = Result,
            N = size(list_to_tuple(R)),
            Res = [R,N],
            Res
    end,

    
   [Li, _] = Query,
   List = get_list_features(Li),
   ListofF = lists:partition(fun(A)->length(A)==1end,lists:map(fun(Y)->
                lists:filter(fun(X)->Y==X end,List)end,lists:usort(List))),
   {Less,Most} = ListofF,
   Le = uniques(Less),
   Mo = uniques(Most),
   Info = {{"Less use:"},[Le],{"Most use:"},[Mo]},
   io:format("~p", [Info]).


most_problems(By)->
    Pid = setup_data:connect(),
    Query =  case By  of
        "version" ->
            {ok, Result} = riakc_pb_socket:get_index_eq(Pid,
                list_to_binary("Product"),
                {binary_index, "error"},
                list_to_binary("TypeError")),
            riakc_pb_socket:stop(Pid),
            {_,R,_,_} = Result,
            get_list_versions(R);
        "feature"->
            {ok, Result} = riakc_pb_socket:get_index_eq(Pid,
                list_to_binary("Product"),
                 {binary_index, "error"},
                 list_to_binary("TypeError")),
            riakc_pb_socket:stop(Pid),
            {_,R,_,_} = Result,
            get_list_features(R)
    end,

    
   ListofF = lists:partition(fun(A)->length(A)==1end,lists:map(fun(Y)->
                lists:filter(fun(X)->Y==X end,Query)end,lists:usort(Query))),
   {_,Most} = ListofF,
   Mo = uniques(Most),
   Info = {{"Most:"},[Mo]},
   io:format("~p", [Info]).


%% Helpers to extract desired value and get unique lists of data
get_list_features(L)->
        T = lists:foldl(fun(X,A2) ->
                    D = binary_to_list(X),
                    C = string:tokens(D, ":"),
                    [_,_,_,A] = C,
                    lists:append([A2,[A]])
            end, [], L),
            
        T.
get_list_versions(L)->
        T = lists:foldl(fun(X,A2) ->
                    D = binary_to_list(X),
                    C = string:tokens(D, ":"),
                    [_,_,A,_] = C,
                    lists:append([A2,[A]])
            end, [], L),
            
        T.
uniques(L)->
     T = lists:foldl(fun(X,A2) ->
                List = lists:usort(X),
                lists:append([A2,[List]])
        end, [], L),
    T.
