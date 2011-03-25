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

-export([with_tx/2,new_tx/2,with_cursor/2]).

-define(CURRENT_TX, bdb_current_tx).

new_tx(Env,Fun) ->
    ParrentTX = erlang:get(?CURRENT_TX),
    {ok, Txn} = bdb_nifs:txn_begin(Env, ParrentTX, []),
    try
        erlang:put(?CURRENT_TX, Txn),
        Result = Fun(Txn),
        ok = bdb_nifs:txn_commit(Txn),
        Result
    catch
        Class:Reason ->
            bdb_nifs:txn_abort(Txn),
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
                    {ok, Cursor} = bdb_nifs:cursor_open(Store, TX, []),
                    try
                        Fun(Cursor)
                    after
                        bdb_nifs:cursor_close(Cursor)
                    end
            end).
