%%
%% Copyright 2014 Joaquim Rocha
%% 
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%

-module(gdb_bucket_util).

-define(OPEN_OPTIONS, [
		{create_if_missing, true},
		{write_buffer_size, 31457280},
		{max_open_files, 30},
		{sst_block_size, 4096},
		{block_restart_interval, 16},
		{cache_size, 8388608},
		{verify_compactions, true},
		{compression, true},
		{use_bloomfilter, true}]).

-define(WRITE_OPTIONS, [{sync, false}]).

-define(READ_OPTIONS, [{verify_checksums, true}]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([close_bucket/1, open_bucket/2,	delete_bucket/1]).

-export([get/2,	put/3, delete/2]).

-export([batch_exec/2, batch_put/3,	batch_delete/2]).

-spec open_bucket(BucketPath :: string(), Config :: list()) -> {ok, BRef :: binary()} | {error, Reason :: term()}.
open_bucket(BucketPath, _Config) ->
	case eleveldb:open(BucketPath, ?OPEN_OPTIONS) of
		{ok, BRef} -> {ok, BRef};
		Error -> Error
	end.

-spec close_bucket(BRef :: term()) -> ok | {error, Reason :: term()}.
close_bucket(BRef) ->
	eleveldb:close(BRef).

-spec delete_bucket(BucketPath :: string()) -> ok | {error, Reason :: term()}.
delete_bucket(BucketPath) ->
	eleveldb:destroy(BucketPath, []).

-spec get(BRef :: binary(), Key :: term()) -> {ok, term()} | not_found | {error, term()}.
get(BRef, Key) ->
	BinKey = term_to_binary(Key),
	case eleveldb:get(BRef, BinKey, ?READ_OPTIONS) of
		{ok, BinValue} ->
			Value = binary_to_term(BinValue),
			{ok, Value};
		Other ->  Other
	end.	

-spec put(BRef :: binary(), Key :: term(), Value :: term()) -> ok | {error, term()}.
put(BRef, Key, Value) ->
	BinKey = term_to_binary(Key),
	BinValue = term_to_binary(Value),
	eleveldb:put(BRef, BinKey, BinValue, ?WRITE_OPTIONS).

-spec delete(BRef :: binary(), Key :: term()) -> ok | {error, term()}.
delete(BRef, Key) ->
	BinKey = term_to_binary(Key),
	eleveldb:delete(BRef, BinKey, ?WRITE_OPTIONS).

-spec batch_exec(BRef :: binary(), Cmds :: list()) -> ok | {error, term()}.
batch_exec(BRef, Cmds) ->
	eleveldb:write(BRef, Cmds, ?WRITE_OPTIONS).

-spec batch_put(Key :: term(), Value :: term(), Cmds :: list()) -> Cmds1 :: list().
batch_put(Key, Value, Cmds) ->
	BinKey = term_to_binary(Key),
	BinValue = term_to_binary(Value),
	Cmd = {put, BinKey, BinValue},
	[Cmd|Cmds].

-spec batch_delete(Key :: term(), Cmds :: list()) -> Cmds1 :: list().
batch_delete(Key, Cmds) ->
	BinKey = term_to_binary(Key),
	Cmd = {delete, BinKey},
	[Cmd|Cmds].

%% ====================================================================
%% Internal functions
%% ====================================================================


