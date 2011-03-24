-module(ebdb).

-export([with_tx/2]).

with_tx(Fun,Env) ->
    {ok, Txn} = ebdb_nifs:txn_begin(Env),
    try 
        Result = Fun(Txn),
        ebdb_nifs:txn_commit(Txn),
        Result
    catch
        Class:Reason ->
            ebdb_nifs:txn_abort(Txn),
            erlang:raise(Class, Reason, erlang:get_stacktrace())
    end.
        
