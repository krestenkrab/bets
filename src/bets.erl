-module(bets).
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

%%@doc
%% BETS is an ETS-lookalike, based on the Erlang Berkeley DB API
%%@end
-export([open/2,insert/2,lookup/2,close/1,close/2,match/2,select/2,fold/3]).

%-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
%-endif.

-record(db, {
          env                :: bdb_nifs:env(),
          store              :: bdb_nifs:db(),
          keypos = 1         :: pos_integer(),
          duplicates = false :: boolean(),
          data_file          :: string()
         }).

process_options(Options, DB, Flags, Method) ->
    case Options of
        [set|R] ->
            process_options(R, DB#db{ duplicates = false }, Flags, hash);
        [bag|R] ->
            process_options(R, DB#db{ duplicates = true }, Flags, hash);
        [ordered_set|R] ->
            process_options(R, DB#db{ duplicates = false }, Flags, btree);
        [ordered_bag|R] ->
            process_options(R, DB#db{ duplicates = true }, Flags, btree);
        [{keypos,N}|R] ->
            process_options(R, DB#db{ keypos=N }, Flags, Method);

        %%
        %% pass-thru for BerkeleyDB options
        %%
        [OPT|R] when is_atom(OPT) ->
            process_options(R, DB, [OPT | Flags ], Method );
        [{OPT,true}|R] when is_atom(OPT) ->
            process_options(R, DB, [OPT | Flags ], Method );
        [{OPT,false}|R] when is_atom(OPT) ->
            process_options(R, DB, [E || E <- Flags, E =/= OPT ], Method );

        [] ->
            {DB, Flags, Method}
    end.

open(Directory, Options) ->
    DefaultFlags = [create,init_txn,recover,init_mpool,thread],
    {DB,Flags,Method} = process_options(Options, #db{}, DefaultFlags, hash),
    {ok, Env} = bdb_nifs:env_open(Directory, Flags),
    {ok, Store} =
%        bdb:with_tx
%          (Env, fun(TX) ->
                        bdb_nifs:db_open(Env,
                                          undefined,
                                          "data.db", "main",
                                          Method,
                                          DB#db.duplicates,
                                          [create,thread,auto_commit])
%                end)
        ,
    {ok, DB#db{ env=Env, store=Store, data_file=filename:join(Directory, "data.db") }}.

close(#db{}=DB) ->
    close(DB, sync).

-spec close(#db{}, sync|nosync) -> ok | {error, term()}.
close(#db{}=DB, sync) ->
    bdb_nifs:db_close(DB#db.store, []);

close(#db{}=DB, nosync) ->
    bdb_nifs:db_close(DB#db.store, [nosync]).

insert(#db{ keypos=KeyIndex, store=Store }, Tuple) ->
    Key = element(KeyIndex, Tuple),
    BinKey = sext:encode(Key),
    bdb_nifs:db_put(Store, undefined, BinKey, term_to_binary(Tuple), []).

lookup(#db{ duplicates=Dups }=DB, Key) ->
    BinKey = sext:encode(Key),
    case Dups of
        false ->
            case bdb_nifs:db_get(DB#db.store, undefined, BinKey, []) of
                {ok, BinTuple} ->
                    [binary_to_term(BinTuple)];
                {error, notfound} ->
                    [];
                {error, E} ->
                    exit(E)
            end;

        true ->
            lists:reverse(lookup_duplicates(DB,BinKey))
    end.

lookup_duplicates(DB,BinKey) ->
    {ok, TX} = bdb_nifs:txn_begin(DB#db.env, [read_committed]),
    try
        {ok, Cursor} = bdb_nifs:cursor_open(DB#db.store, TX, []),
        try bdb_nifs:cursor_get(Cursor, BinKey, [set]) of
            {ok, _, BinTuple} ->
                lookup_next_dups(Cursor, BinKey, [binary_to_term(BinTuple)]);
            {error, notfound} ->
                [];
            {error, _} = E ->
                E
        after
            ok = bdb_nifs:cursor_close(Cursor)
        end
    after
        %% just abort it, it's a read-only txn anyway
        bdb_nifs:txn_abort(TX)
    end.

lookup_next_dups(Cursor, BinKey, Rest) ->
    case bdb_nifs:cursor_get(Cursor, BinKey, [next_dup]) of
        {ok, BinKey, BinTuple} ->
            lookup_next_dups(Cursor, BinKey, [binary_to_term(BinTuple) | Rest]);
        {ok, _, _} ->
            Rest;
        {error, notfound} ->
            Rest;
        {error, keyempty} ->
            Rest;
        {error, _} = E ->
            E;
        X -> exit({'huh?', X})
    end.

match(DB, Pattern) ->
    select(DB, [{Pattern,[],['$$']}]).

select(#db{keypos=KeyIndex}=DB, MatchSpec) ->
    case MatchSpec of
        [] -> [];
        [{Pattern,_,_}]=MSE ->
            CMS = ets:match_spec_compile(MSE),
            KeyPattern = element(KeyIndex, Pattern),
            KeyPrefix = sext:prefix(KeyPattern),

            Result = bdb:fold(fun(BinTuple, Acc0) ->
                                  Tuple = erlang:binary_to_term(BinTuple),
                                  io:format("tup: ~p~n", [Tuple]),
                                  case ets:match_spec_run([Tuple], CMS) of
                                      [] -> Acc0;
                                      [[Data]] -> [Data | Acc0]
                                  end
                          end,
                          [],
                          KeyPrefix,
                          DB),
            lists:reverse(Result);

        MatchSpec ->
            CMS = ets:match_spec_compile(MatchSpec),
            Result = fold(fun(Tuple, Acc0) ->
                                  case ets:match_spec_run([Tuple], CMS) of
                                      [] -> Acc0;
                                      [[Data]] -> [Data | Acc0]
                                  end
                          end,
                          [],
                          DB),
            lists:reverse(Result)
    end.

fold(Fun,Acc,DB) ->
    bdb:fold(fun(Bin,A0)-> Fun(erlang:binary_to_term(Bin),A0) end,Acc,<<>>,DB).



%-ifdef(TEST).

%% ready for testing!

create_db() ->
    {ok, DB} = open("/tmp/xxx", [{create,true},ordered_bag]),
    insert(DB, {{<<"ab">>,1}, a}),
    insert(DB, {{<<"ac">>,2}, b}),
    insert(DB, {{<<"ac">>,2}, b2}),
    insert(DB, {{<<"ac">>,3}, c}),
    insert(DB, {{<<"ac">>,4}, x}),
    insert(DB, {{<<"ac">>,5}, c}),
    insert(DB, {{<<"bc">>,4}, d}),
    DB.

remove_db(DB) ->
    close(DB),
    ok = bdb_nifs:db_remove(DB#db.env,
                             undefined,
                             DB#db.data_file,
                             "main",
                             [auto_commit]).

simple_test() ->

    DB = create_db(),

    try
        [{{<<"ac">>,2},b},{{<<"ac">>,2},b2}] = lookup(DB, {<<"ac">>,2}),
        [] = lookup(DB, {<<"ac">>,0}),

        [b,b2,c,x,c] = match(DB, {{<<"ac">>, '_'}, '$1'})

        %%x = fold(fun(V,Acc)->[V|Acc]end,[],DB),
        %%7 = length(List)
    after
        remove_db(DB)
    end.


%-endif.

