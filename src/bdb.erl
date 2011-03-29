-module(bdb).
%%
%% This file is part of BETS - Erlang Berkeley DB API
%%
%% Copyright (c) 2011 by Trifork
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%

-export([fold/4]).

-export([transactional/2,transaction/2,with_cursor/2,current/1]).

-include("bdb_internal.hrl").

transaction(#db{env=Env},Fun) ->

    OldTX = erlang:get(?CURRENT_TX),
    
    {ok, Txn} = bdb_nifs:txn_begin(Env, txfind(Env,OldTX), []),
    try
        erlang:put(?CURRENT_TX, [{Env,Txn}|OldTX]),
        Result = Fun(),
        ok = bdb_nifs:txn_commit(Txn),
        Result
    catch
        Class:Reason ->
            bdb_nifs:txn_abort(Txn),
            erlang:raise(Class, Reason, erlang:get_stacktrace())
    after
        erlang:put(?CURRENT_TX, OldTX)
    end.

current(#db{env=Env}) ->
    txfind(Env, erlang:get(?CURRENT_TX)).


txfind(E,[{E,V}|_]) ->
    V;
txfind(E,[{_,_}|R]) ->
    txfind(E,R);
txfind(_,_) ->
    undefined.


transactional(DB,Fun) ->
    case current(DB) of
        undefined -> transaction(DB,Fun);
        Txn       -> Fun(Txn)
    end.

with_cursor(#db{store=Store}=DB,Fun) ->
    {ok, Cursor} = bdb_nifs:cursor_open(Store, current(DB), []),
    try
        Fun(Cursor)
    after
        bdb_nifs:cursor_close(Cursor)
    end.

%%@doc
%% fold/4 folds over all elements with a given prefix
%%@end
fold(Fun,Acc,KeyPrefix,#db{}=DB) when is_binary(KeyPrefix) ->
    PrefixLen = byte_size(KeyPrefix),
    with_cursor (DB,
       fun(Cursor) ->
               case bdb_nifs:cursor_get(Cursor, KeyPrefix, [set_range]) of
                   {ok, <<KeyPrefix:PrefixLen/binary, _/binary>>=BinKey, BinValue} ->
                       Acc1 = Fun(BinValue,Acc),
                       case DB#db.duplicates of
                           true ->
                               Acc2 = fold_dups(Cursor, BinKey, Fun, Acc1);
                           false ->
                               Acc2 = Acc1
                       end,

                       fold_prefix_next(DB, Fun, Cursor, KeyPrefix, PrefixLen, Acc2);

                   {ok, _, _} ->
                       [];
                   {error, notfound} ->
                       [];
                   {error, keyempty} ->
                       [];
                   {error, _} = E ->
                       exit(E)
               end
       end).

fold_dups(Cursor, BinKey, Fun, Acc) ->
    case bdb_nifs:cursor_get(Cursor, BinKey, [next_dup]) of
        {ok, BinKey, BinValue} ->
            Acc1 = Fun(BinValue, Acc),
            fold_dups(Cursor, BinKey, Fun, Acc1);
        {ok, _, _} ->
            Acc;
        {error, notfound} ->
            Acc;
        {error, keyempty} ->
            Acc;
        {error, _} = E ->
            exit(E)
    end.

fold_prefix_next(DB, Fun, Cursor, KeyPrefix, PrefixLen, Acc) ->
    case bdb_nifs:cursor_get(Cursor, <<>>, [next]) of
        {ok, <<KeyPrefix:PrefixLen/binary, _/binary>>=BinKey, BinValue} ->
            Acc1 = Fun(BinValue,Acc),

            case DB#db.duplicates of
                true ->
                    Acc2 = fold_dups(Cursor, BinKey, Fun, Acc1);
                false ->
                    Acc2 = Acc1
            end,

            fold_prefix_next(DB, Fun, Cursor, KeyPrefix, PrefixLen, Acc2);
        {ok, _, _} ->
            Acc;
        {error, notfound} ->
            Acc;
        {error, keyempty} ->
            Acc;
        {error, _} = E ->
            exit(E)
    end.

