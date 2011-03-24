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
-export([open/2,insert/2,lookup/2,close/1,close/2,match/2,select/2,fold/3,fold/4]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(db, { 
          env                :: ebdb_nifs:env(),
          store              :: ebdb_nifs:db(),
          keypos = 1         :: pos_integer(),
          duplicates = false :: boolean()
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
    {ok, Env} = ebdb_nifs:env_open(Directory, 
                                      Flags),
    {ok, Store} = ebdb_nifs:db_open(Env, undefined, "data.db", 
                                    Method, DB#db.duplicates, [create,thread,auto_commit]),
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
    ebdb_nifs:db_put(Store, undefined, BinKey, term_to_binary(Tuple), []).

lookup(#db{ duplicates=Dups }=DB, Key) ->
    BinKey = sext:encode(Key),
    case Dups of
        false ->
            case ebdb_nifs:db_get(DB#db.store, undefined, BinKey, []) of
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
    {ok, TX} = ebdb_nifs:txn_begin(DB#db.env, [read_committed]),
    try
        {ok, Cursor} = ebdb_nifs:cursor_open(DB#db.store, TX, []),
        try ebdb_nifs:cursor_get(Cursor, BinKey, [set]) of
            {ok, _, BinTuple} ->
                lookup_next_dups(Cursor, BinKey, [binary_to_term(BinTuple)]);
            {error, notfound} ->
                [];
            {error, _} = E ->
                E
        after
            ok = ebdb_nifs:cursor_close(Cursor)
        end
    after
        %% just abort it, it's a read-only txn anyway
        ebdb_nifs:txn_abort(TX)
    end.

lookup_next_dups(Cursor, BinKey, Rest) ->
    case ebdb_nifs:cursor_get(Cursor, BinKey, [next_dup]) of
        {ok, BinKey, BinTuple} ->
            lookup_next_dups(Cursor, BinKey, [binary_to_term(BinTuple) | Rest]);
        {ok, _, _} ->
            Rest;
        {error, notfound} ->
            Rest;
        {error, keyempty} ->
            Rest;
        {error, _} = E ->
            E
    end.

fold_dups(Cursor, BinKey, Fun, Acc) ->
    case ebdb_nifs:cursor_get(Cursor, BinKey, [next_dup]) of
        {ok, _, BinTuple} ->
            fold_dups(Cursor, BinKey, Fun, Fun( binary_to_term(BinTuple), Acc));
        {error, notfound} ->
            Acc;
        {error, keyempty} ->
            Acc;
        {error, _} = E ->
            E
    end.

match(DB, Pattern) ->
    select(DB, [{Pattern,[],['$$']}]).

select(#db{keypos=KeyIndex}=DB, MatchSpec) ->
    lists:foldl(fun({Pattern,_,_}=MSE, Acc) ->
                        CMS = ets:match_spec_compile([MSE]),
                        KeyPattern = element(KeyIndex, Pattern),
                        KeyPrefix = sext:prefix(KeyPattern),
                        
                        Result = fold(fun(Tuple, Acc0) ->
                                              case ets:match_spec_run([Tuple], CMS) of
                                                  [] -> Acc0;
                                                  [Res] -> Res ++ Acc0
                                              end
                                      end,
                                      Acc,
                                      KeyPrefix,
                                      DB),
                        lists:reverse(Result)
                end,
                [],
                MatchSpec).

fold(Fun,Acc,DB) ->
    fold(Fun,Acc,<<>>,DB).

%%@doc 
%% fold/4 folds over all elements with a given prefix
%%@end
fold(Fun,Acc,KeyPrefix,#db{}=DB) when is_binary(KeyPrefix) ->
    PrefixLen = byte_size(KeyPrefix),
    edbd:with_tx
      (fun(TX) ->
        {ok, Cursor} = ebdb_nifs:cursor_open(DB#db.store, TX, []),

        try ebdb_nifs:cursor_get(Cursor, KeyPrefix, [set_range]) of
            {ok, <<KeyPrefix:PrefixLen/binary, _/binary>>=BinKey, BinValue} ->
                Value = erlang:binary_to_term(BinValue),
                Acc1 = Fun(Value,Acc),
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
                E
        after
            ok = ebdb_nifs:cursor_close(Cursor)
        end
       end,
       DB#db.env).
    

fold_prefix_next(DB, Fun, Cursor, KeyPrefix, PrefixLen, Acc) ->
    case ebdb_nifs:cursor_get(Cursor, <<>>, [next]) of
        {ok, <<KeyPrefix:PrefixLen/binary, _/binary>>=BinKey, BinValue} ->
            Value = binary_to_term(BinValue),
            Acc1 = Fun(Value,Acc),
            
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
            Acc
    end.
                       

    
-ifdef(TEST).

%% ready for testing!

simple_test() ->
    
    {ok, DB} = open("/tmp", [{create,true}]),
    insert(DB, {{<<"ab">>,1}, a}),
    insert(DB, {{<<"ac">>,2}, b}),
    insert(DB, {{<<"ac">>,3}, c}),
    insert(DB, {{<<"bc">>,4}, d}),

    [{{<<"ac">>, 2}, b}] = lookup(DB, {<<"ac">>,2}),
    
%%    x = match(DB, {{<<"ac">>, '_'}, '$1'}),

    close(DB).
    

-endif.

