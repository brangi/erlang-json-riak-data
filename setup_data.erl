-module(setup_data).
-export([
        setup_data/0,
        msToDate/1,
        use/0,
        clean/0,
        connect/0,
        get_values_from_json/1]).


-define(P , "/root/riak-erlang-client/seg").


use()->
    numeroyslashenstring.

setup_data() ->

   %% Sets = [segai,  segaj,  segak,  segal,  segam,  segan,  segao,  segap,  segaq,
   %%     segar,  segas,  segat],
    Sets = ["segaa",  "segac",  "segae",  "segag",  "segai",  "segak",  "segam",  "segao",  "segaq",  "segas",
    "segab",  "segad",  "segaf",  "segah",  "segaj",  "segal",  "segan",  "segap",  "segar",  "segat"],

    Path = ["2/","3/","4/","5/","6/","7/","8/","9/","10/"],

    lists:foreach(fun(Pa) ->
        lists:foreach(fun(Name) ->
                os:cmd("riak start"),
                timer:sleep(1000),
                {ok, Device} = file:open(?P++Pa++Name, [read]),
                for_each_line(Device),
                os:cmd("riak stop"),
                 timer:sleep(10000)
        end, Sets)
    end, Path).
 
for_each_line(Device) ->
    case io:get_line(Device, "") of
        eof  -> file:close(Device);
        Line ->
                L = mochijson:decode(Line),
                JsonData = get_values_from_json(L),

                BN = <<"Product">>,
                KeyD = key_data(JsonData),
                set_indexed_bucket(BN, KeyD, JsonData),
                %timer:sleep(500),
		io:format("LINE :  ......  ~p~n",[L]),
		for_each_line(Device)
    end.

connect()->
        {ok, Pid} = riakc_pb_socket:start_link("127.0.0.1", 8087),
        Pid.
clean()->
    Pid = connect(),
    {ok, B} = riakc_pb_socket:list_buckets(Pid),

    lists:foreach(fun(X) ->
             {ok, K} = riakc_pb_socket:list_keys(Pid,X),
                lists:foreach(fun(Y) ->
                            riakc_pb_socket:delete(Pid, X, Y)
                    end, K)
        end,B).



key_data(Info)->
    [_, _, Id, OS, Ver, _ ,Fet] = Info,
    O = binary_to_list(OS),
    V = binary_to_list(Ver),
    F = binary_to_list(Fet),
    I = binary_to_list(Id), 
    list_to_binary(I++":"++O++":"++V++":"++F).

get_search_indexes(Info) ->
    [Date,Product, Id,OS, Version, Error,Feature] = Info,
    
    %%Date Index value
    DT = list_to_binary(get_str_list(Date)),
   
    %%Get list of strings splited
    DateSplit =  get_list_str(Date),

    %%Extract values
    [Year,_,_] =DateSplit,
    [_,Month,_] = DateSplit,
    [_,_,Day] = DateSplit,

    %%Conver to binary
    Y = list_to_binary(Year),
    M = list_to_binary(Month),
    D = list_to_binary(Day),

    
    [Product,Id,DT,Y,M,D,OS,Version,Error,Feature].

set_indexes(Bucket, Values)->
    O1 = riakc_obj:get_update_metadata(Bucket),

    V = get_search_indexes(Values),
    [P,I,DT,Y,M,D,OS,Ver,E,Fet] = V,

   Indexes =[
        {{binary_index, "uuid"}, [I]},
        {{binary_index, "product"}, [P]},
        {{binary_index, "date"}, [DT]},
        {{binary_index, "year"}, [Y]},
        {{binary_index, "month"}, [M]},
        {{binary_index, "day"}, [D]},
        {{binary_index, "os"}, [OS]},
        {{binary_index, "version"}, [Ver]},
        {{binary_index, "error"}, [E]},
        {{binary_index, "feature"}, [Fet]}
    
    ],
    O2 = riakc_obj:set_secondary_index(O1, Indexes),
    riakc_obj:update_metadata(Bucket,O2).

set_indexed_bucket(Name, Key, V) ->
    Pid= connect(),
    N = case is_binary(Name) of
        false ->
            list_to_binary(Name);
        true ->
            Name
    end,
    K = case is_binary(Key) of
        false ->
            list_to_binary(Key);
        true ->
            Key
    end,
    Data =  key_data(V),
    Obj = riakc_obj:new(N, K, Data),
    Obj2 = set_indexes(Obj, V),
    riakc_pb_socket:put(Pid, Obj2),
    riakc_pb_socket:stop(Pid).

get_values_from_json(Struct)->
    {struct, JD1}  = Struct,
    V1 = case proplists:get_value("timestamp", JD1) of
        undefined -> <<"none">>;
        _->
            msToDate(proplists:get_value("timestamp", JD1))
    end,
    {struct, Payload} = proplists:get_value("payload", JD1),
    
    V2 = case proplists:get_value("product",Payload) of
        undefined -> <<"none">>;
        _->
           list_to_binary(proplists:get_value("product",Payload))
    end,

    V3 = case proplists:get_value("uuid",Payload) of
        undefined -> <<"none">>;
        _->
            list_to_binary(proplists:get_value("uuid",Payload))
    end,
    V4 = case proplists:get_value("platform",Payload) of
        undefined -> <<"none">>;
        _->
            list_to_binary(proplists:get_value("platform",Payload))
    end,

    V5 = case proplists:get_value("version",Payload) of
        undefined -> <<"none">>;
        _->
            list_to_binary(proplists:get_value("version",Payload))
    end,

    V6 = case proplists:get_value("error",Payload) of
        undefined -> <<"none">>;
        _->
            list_to_binary(proplists:get_value("error",Payload))
    end,


    V7 = case proplists:get_value("event",Payload) of
        undefined -> <<"none">>;
        _->
            list_to_binary(proplists:get_value("event",Payload))
    end,

   [V1, V2, V3, V4, V5, V6 ,V7].

msToDate(M) ->
    {Date,_} = calendar:gregorian_seconds_to_datetime(
        date_util:epoch_to_gregorian_seconds(M)),
    tuple_to_list(Date).
    
get_list_str(L)->
    T = lists:foldl(fun(X,A2) ->
                A = integer_to_list(X),
                lists:append([A2,[A]])
        end, [], L),
    T.

get_str_list(L)->
    T = lists:foldl(fun(X,A2) ->
                A = integer_to_list(X),
                lists:append([A2,[A]])
        end, [], L),
    S = string:join(T,","),
    S.

