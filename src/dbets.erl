-module(dbets).
%%
%% This file is part of EBDB - Erlang Berkeley DB API
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

%%@doc
%% DB-ETS is an ETS-lookalike, based on the Erlang Berkeley DB API
%%@end
-export([open/2,insert/2,lookup/2,close/1,close/2]).

-record(db, { 
          env                :: edbd_nifs:env(),
          store              :: edbd_nifs:db(),
          keypos = 1         :: pos_integer(),
          method = hash      :: hash|btree,
          duplicates = false :: boolean()
         }).

process_options(Options, DB, Flags) ->
    case Options of
        [set|R] ->
            process_options(R, DB#db{ method = hash, duplicates = false }, Flags);
        [ordered_set|R] ->
            process_options(R, DB#db{ method = btree, duplicates = false }, Flags);
        [bag|R] ->
            process_options(R, DB#db{ method = hash, duplicates = true }, Flags);
        [ordered_bag|R] ->
            process_options(R, DB#db{ method = btree, duplicates = false }, Flags);
        [{keypos,N}|R] ->
            process_options(R, DB#db{ keypos=N }, Flags);

        %%
        %% pass-thru for BerkeleyDB options
        %%
        [OPT|R] when is_atom(OPT) ->
            process_options(R, DB, [OPT | Flags ] );
        [{OPT,true}|R] when is_atom(OPT) ->
            process_options(R, DB, [OPT | Flags ] );
        [{OPT,false}|R] when is_atom(OPT) ->
            process_options(R, DB, [E || E <- Flags, E =/= OPT ] );

        [] ->
            {DB, Flags}
    end.

open(Directory, Options) ->
    DefaultFlags = [create,init_txn,recover,init_mpool,thread],
    {DB,Flags} = process_options(Options, #db{}, DefaultFlags),
    {ok, Env} = edbd_nifs:db_open_env(Directory, 
                                      Flags),
    {ok, Store} = edbd_nifs:db_open(Env, undefined, "data.db", DB#db.method, [create,thread]),
    {ok, DB#db{ env=Env, store=Store }}.

close(#db{}=DB) ->
    close(DB, sync).

-spec close(#db{}, sync|nosync) -> ok | {error, term()}.
close(#db{}=DB, sync) ->
    ebdb_nifs:db_close(DB#db.store, []);

close(#db{}=DB, nosync) ->
    ebdb_nifs:db_close(DB#db.store, [nosync]).

insert(#db{ keypos=KeyIndex, store=Store }, Tuple) ->
    Key = element(KeyIndex, Tuple),
    BinKey = sext:encode(Key),
    edbd_nifs:db_put(Store, undefined, BinKey, term_to_binary(Tuple), [auto_commit]).

lookup(#db{ duplicates=Dups }=DB, Key) ->
    BinKey = sext:encode(Key),
    case Dups of
        false ->
            case edbd_nifs:db_get(DB#db.store, undefined, BinKey, []) of
                {ok, BinTuple} ->
                    [binary_to_term(BinTuple)];
                {error, notfound} ->
                    [];
                {error, E} ->
                    exit(E)
            end;

        true ->
            lookup_duplicates(DB,BinKey)
    end.

lookup_duplicates(DB,BinKey) ->
    {ok, TX} = edbd_nifs:txn_begin(DB#db.env, [read_committed]),
    try
        {ok, Cursor} = edbd_nifs:cursor_open(DB#db.store, TX, []),
        try edbd_nifs:cursor_set(Cursor, BinKey) of
            {ok, BinTuple} ->
                lookup_next_dups(Cursor, [binary_to_term(BinTuple)]);
            {error, notfound} ->
                [];
            {error, _} = E ->
                E
        after
            ok = edbd_nifs:cursor_close(Cursor)
        end
    after
        %% just abort it, it's a read-only txn anyway
        edbd_nifs:txn_abort(TX)
    end.

lookup_next_dups(Cursor, Rest) ->
    case edbd_nifs:cursor_next_dup(Cursor) of
        {ok, BinTuple} ->
            lookup_next_dups(Cursor, [binary_to_term(BinTuple) | Rest]);
        {error, notfound} ->
            Rest;
        {error, _} = E ->
            E
    end.



    
