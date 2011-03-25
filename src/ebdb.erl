-module(ebdb).

-export([with_tx/2,new_tx/2,with_cursor/2]).

-define(CURRENT_TX, ebdb_current_tx).

new_tx(Env,Fun) ->
    ParrentTX = erlang:get(?CURRENT_TX),
    {ok, Txn} = ebdb_nifs:txn_begin(Env, ParrentTX, []),
    try
        erlang:put(?CURRENT_TX, Txn),
        Result = Fun(Txn),
        ok = ebdb_nifs:txn_commit(Txn),
        Result
    catch
        Class:Reason ->
            ebdb_nifs:txn_abort(Txn),
            erlang:raise(Class, Reason, erlang:get_stacktrace())
    after
        erlang:put(?CURRENT_TX, ParrentTX)
    end.


with_tx(Env,Fun) ->
    case erlang:get(?CURRENT_TX) of
        undefined ->
            new_tx(Env,Fun);

        Txn ->
            Fun(Txn)
    end.

with_cursor({db,Env,Store,_KeyPos,_Dups,_File},Fun) ->
    with_tx(Env,
            fun(TX) ->
                    {ok, Cursor} = ebdb_nifs:cursor_open(Store, TX, []),
                    try
                        Fun(Cursor)
                    after
                        ebdb_nifs:cursor_close(Cursor)
                    end
            end).
