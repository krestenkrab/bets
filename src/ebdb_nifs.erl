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

-module(ebdb_nifs).

-export([db_open_env/2, db_open/6, db_close/2]).
-export([db_get/4]).
-export([db_put/5]).
-export([cursor_open/3, cursor_close/1]).
-export([txn_begin/1,txn_begin/2,txn_begin/3,txn_commit/1,txn_commit/2,txn_abort/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(missing_nif, exit(missing_nif)).
-define(NOTXN, undefined). 
-define(NOENV, undefined). 

%%
%% public specs

-opaque db()  :: term().
-opaque env() :: term().
-opaque txn() :: term().
    

-on_load(init/0).

-spec init() -> ok | {error, any()}.

init() ->
    case code:priv_dir(?MODULE) of
        {error, bad_name} ->
            SoName = filename:join("../priv", atom_to_list(?MODULE));
        Dir ->
            SoName = filename:join(Dir, atom_to_list(?MODULE))
    end,
    erlang:load_nif(SoName, 0).

-type env_open_flag() :: 
       init_cdb | init_lock | init_log | init_mpool | init_rep | init_txn
     | recover | recover_fatal
     | use_environ | use_environ_root
     | create | lockdown | private | register | system_mem | thread .

-type db_open_flag() ::
       auto_commit | create | excl | multiversion | nommap | rdonly 
     | read_uncommitted | thread | truncate .

-type db_close_flag() :: nosync.

-type access_method() :: btree | hash | queue | recno | unknown.

-spec db_open_env( string(), [ env_open_flag() ] ) -> env().
db_open_env(_EnvHomeDir, _OpenFlags) ->
    ?missing_nif.

-spec db_open(env()|?NOENV, txn()|?NOTXN, string(), access_method(), boolean(), [ db_open_flag() ]) -> {ok, db()} | {error, term()}.
db_open(_Env, _Txn, _FileName, _AccessMethod, _AllowDups, _OpenFlags) ->
    ?missing_nif.

-spec db_close(db(), [ db_close_flag() ]) -> {error, term()} | ok.
db_close(_DB, _Flags) ->
    ?missing_nif.

-type db_get_flag() :: consume | consume_wait | read_committed | read_uncommitted | rwm.
-spec db_get(db(), txn()|?NOTXN, binary(), [ db_get_flag() ]) -> 
    {ok, binary()} | {error, term()}. 
    
db_get(_DB, _Txn, _KeyBin, _Flags) ->
    ?missing_nif.

-type db_put_flag() :: append | nodupdata | nooverwrite | overwrite_dup.
-spec db_put(db(), txn()|?NOTXN, binary(), binary(), [ db_put_flag() ]) -> 
    {ok, binary()} | {error, term()}. 
    
db_put(_DB, _Txn, _KeyBin, _DataBin, _Flags) ->
    ?missing_nif.

-type begin_txn_flag() ::
       read_committed | read_uncommitted | txn_bulk | txn_snapshot
     | txn_nosync | txn_sync
     | txn_nowait | txn_wait | txn_write_nosync .

-spec txn_begin(env()) -> {ok, txn()} | {error, term()}.
-spec txn_begin(env(), [ begin_txn_flag() ]) -> {ok, txn()} | {error, term()}.
-spec txn_begin(env(), txn()|?NOTXN, [ begin_txn_flag() ]) -> {ok, txn()} | {error, term()}.
    
txn_begin(Env) ->
    txn_begin(Env, ?NOTXN, []).

txn_begin(Env, Flags) ->
    txn_begin(Env, ?NOTXN, Flags).

txn_begin(_Env, _ParentTxn, _Flags) ->
    ?missing_nif.

txn_commit(Txn) ->
    txn_commit(Txn, []).

txn_commit(_Txn, _Flags) ->
    ?missing_nif.

txn_abort(_Txn) ->
    ?missing_nif.


cursor_open(_DB, _Txn, _Flags) ->
    ?missing_nif.

cursor_close(_Cursor) ->
    ?missing_nif.

-ifdef(TEST).

no_env_test() ->
    {ok, _} = db_open(?NOENV, ?NOTXN, "sample_noenv.db", btree, false, [create]).

simple_test() ->
    
    {ok, Env} = db_open_env("/tmp",
                            [init_txn,create,recover,init_mpool,private,thread]),

    {ok, TX} = txn_begin(Env),
    
    {ok, DB} = db_open(Env, TX, "sample.db", hash, false,
                       [create,thread]),

    ok = db_put(DB, TX, <<"key">>, <<"value">>, []),

    ok = txn_commit(TX),

    {ok, <<"value">>} = db_get(DB, ?NOTXN, <<"key">>, []),
    {error, notfound} = db_get(DB, ?NOTXN, <<"key2">>, []),
    
    ok = db_close(DB, [nosync]).
    

-endif.
