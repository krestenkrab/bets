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

-export([fold/3, fold/4, lookup/2]).

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

%%@doc
%% Run `Fun' transactionally on `DB'; either in a new transaction
%% or in the current if there is such.
%%@end
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


-spec fold(fun( (binary(), Acc) -> Acc ), Acc, #db{}) -> Acc.

fold(Fun,Acc,DB) ->
    bdb:fold(fun(Bin,A0) ->
                     Fun(Bin,A0)
             end,
             Acc,
             <<>>,
             DB).

%%@doc
%% fold/4 folds over all elements with a given prefix
%%@spec fold(Fun::fun( (binary(), Acc) -> Acc ), Acc, Prefix::binary(), DB::#db{}) -> Acc
%%@end
-spec fold(Fun::fun( (binary(), Acc) -> Acc ), Acc, Prefix::binary(), DB::#db{}) -> Acc.
fold(Fun,Acc,KeyPrefix,#db{method=btree}=DB) when is_binary(KeyPrefix) ->
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

%%
%% Internal function.  Given Cursor is at some key/value; this folds over
%% duplicate elements with the same key.
%%
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

%%
%% Internal function. Implements fold beyone the first key.
%%
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


-spec lookup(#db{}, binary()) -> [binary()] | {error, _}.

lookup(#db{ duplicates=Dups }=DB, BinKey) ->
    case Dups of
        false ->
            case bdb_nifs:db_get(DB#db.store, undefined, BinKey, []) of
                {ok, BinTuple} ->
                    [BinTuple];
                {error, notfound} ->
                    [];
                {error, _}=E ->
                    E
            end;

        true ->
            lists:reverse(lookup_dups(DB,BinKey))
    end.

lookup_dups(DB,BinKey) ->
    bdb:with_cursor(DB,
      fun(Cursor) ->
        case bdb_nifs:cursor_get(Cursor, BinKey, [set]) of
            {ok, _, BinTuple} ->
                lookup_next_dups(Cursor, BinKey, [BinTuple]);
            {error, notfound} ->
                [];
            {error, _} = E ->
                E
        end
      end).

lookup_next_dups(Cursor, BinKey, Rest) ->
    case bdb_nifs:cursor_get(Cursor, BinKey, [next_dup]) of
        {ok, BinKey, BinTuple} ->
            lookup_next_dups(Cursor, BinKey, [BinTuple | Rest]);
        {ok, _, _} ->
            Rest;
        {error, notfound} ->
            Rest;
        {error, keyempty} ->
            Rest;
        {error, _} = E ->
            E
    end.

