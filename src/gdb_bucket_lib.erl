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

-module(gdb_bucket_lib).

-include("giantdb.hrl").

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

-define(KEY_DATA(Key), {d, Key}).
-define(VALUE_DATA(Data, Indexes), {Data, Indexes}).
-define(KEY_INDEX(Index, Key), {i, Index, Key}).

-define(DEFAULT_INDEXES, []).

%% ====================================================================
%% API functions
%% ====================================================================
-export([close_bucket/1, open_bucket/3,	delete_bucket/1]).

-export([get/2,	put/3, delete/2]).

-export([filter/2, foreach/2]).

-export([run_index/4]).

-spec open_bucket(Bucket :: atom(), BucketPath :: string(), Config :: list()) -> {ok, BInfo :: #bucket_info{}} | {error, Reason :: term()}.
open_bucket(Bucket, BucketPath, _Config) ->
	case eleveldb:open(BucketPath, ?OPEN_OPTIONS) of
		{ok, BRef} ->
			BInfo = #bucket_info{bucket=Bucket, ref=BRef},
			{ok, BInfo};
		Error -> Error
	end.

-spec close_bucket(BInfo :: #bucket_info{}) -> ok | {error, Reason :: term()}.
close_bucket(#bucket_info{ref=BRef}) ->
	eleveldb:close(BRef).

-spec delete_bucket(BucketPath :: string()) -> ok | {error, Reason :: term()}.
delete_bucket(BucketPath) ->
	eleveldb:destroy(BucketPath, []).

-spec get(BInfo :: #bucket_info{}, Key :: term()) -> {ok, term()} | not_found | {error, term()}.
get(#bucket_info{ref=BRef}, Key) ->
	case read(BRef, ?KEY_DATA(Key)) of
		{ok, ?VALUE_DATA(Value, _)} -> {ok, Value};
		Other ->  Other
	end.	

-spec put(BInfo :: #bucket_info{}, Key :: term(), Value :: term()) -> ok | {error, term()}.
put(#bucket_info{ref=BRef}, Key, Value) ->
	Cmds = put_data(Key, Value, ?DEFAULT_INDEXES, []),
	batch_exec(BRef, Cmds).

-spec delete(BInfo :: #bucket_info{}, Key :: term()) -> ok | {error, term()}.
delete(#bucket_info{ref=BRef}, Key) ->
	Cmds = delete_data(Key, []),
	batch_exec(BRef, Cmds).

-spec filter(BInfo :: #bucket_info{}, Fun :: FilterFun) -> list()
	when FilterFun :: fun(({Key :: term(), Value :: term()}) -> boolean()).
filter(#bucket_info{ref=BRef}, Fun) ->
	Fold = fun({BinKey, BinValue}, Acc) ->
			KeyT = binary_to_term(BinKey),
			case KeyT of
				?KEY_DATA(Key) ->
					?VALUE_DATA(Value, _) = binary_to_term(BinValue),
					KeyPar = {Key, Value},
					case Fun(KeyPar) of
						true -> [KeyPar|Acc];
						false -> Acc
					end;
				_ -> Acc
			end
	end,
	eleveldb:fold(BRef, Fold, [], ?READ_OPTIONS).

-spec foreach(BInfo :: #bucket_info{}, Fun :: AllFun) -> ok
	when AllFun :: fun(({Key :: term(), Value :: term()}) -> any()).
foreach(#bucket_info{ref=BRef}, Fun) ->
	Fold = fun({BinKey, BinValue}, _Acc) ->
			KeyT = binary_to_term(BinKey),
			case KeyT of
				?KEY_DATA(Key) ->
					?VALUE_DATA(Value, _) = binary_to_term(BinValue),
					KeyPar = {Key, Value},
					Fun(KeyPar),
					ok;
				_ -> ok
			end
	end,
	eleveldb:fold(BRef, Fold, ok, ?READ_OPTIONS).

-spec run_index(BInfo :: #bucket_info{}, Index :: atom(), Module :: atom(), Function :: atom()) -> 
	{ok, BInfo1 :: #bucket_info{}} | {error, term()}.
run_index(BInfo = #bucket_info{ref=BRef}, Index, Module, Function) ->
	Index = #index_info{index=Index, module=Module, function=Function},
	Fold = fun({BinKey, BinValue}, Acc) ->
			KeyT = binary_to_term(BinKey),
			case KeyT of
				?KEY_DATA(Key) ->
					?VALUE_DATA(Value, Indexes) = binary_to_term(BinValue),
					case run_index(Index, Key, Value) of
						{ok, IndexKeys} ->
							case load_index(BInfo, Index, IndexKeys, []) of
								{ok, IndexKeyList} ->
									Indexes1 = lists:keystore(Index, 1, Indexes, {Index, IndexKeys}),
									Cmds = put_data(Key, Value, Indexes1, []),
									Cmds1 = make_add_index_cmds(Key, Index, IndexKeyList, Cmds),
									Cmds1 ++ Acc;
								{error, Error} -> error(Error)
							end;
						{error, Error} -> error(Error)
					end;
				_ -> Acc
			end
	end,
	try
		Cmds = eleveldb:fold(BRef, Fold, [], ?READ_OPTIONS),
		case batch_exec(BRef, Cmds) of
			ok ->
				Indexes = [Index | BInfo#bucket_info.indexes],
				{ok, BInfo#bucket_info{indexes=Indexes}};
			Other -> Other
		end
	catch _:Error -> {error, Error}
	end.

%% ====================================================================
%% Internal functions
%% ====================================================================

make_add_index_cmds(Key, Index, IndexKeyList, Cmds) ->
	lists:foldl(fun({IKey, IList}, Acc) ->
				IList1 = case lists:member(Key, IList) of
					true -> IList;
					false -> [Key|IList]
				end,
				put_index(Index, IKey, IList1, Acc)
		end, Cmds, IndexKeyList).	

load_index(BInf = #bucket_info{ref=BRef}, Index, [H|T], Output) ->
	IKey = ?KEY_INDEX(Index, H),
	case read(BRef, IKey) of
		not_found -> 
			KeyPar = {IKey, []},
			load_index(BInf, Index, T, [KeyPar|Output]);
		{ok, List} ->
			KeyPar = {IKey, List},
			load_index(BInf, Index, T, [KeyPar|Output]);
		Error -> Error
	end;
load_index(_BInfo, _Index, [], Output) -> {ok, Output}.

read(BRef, Key) ->
	BinKey = term_to_binary(Key),
	case eleveldb:get(BRef, BinKey, ?READ_OPTIONS) of
		{ok, BinValue} ->
			Value = binary_to_term(BinValue),
			{ok, Value};
		Other ->  Other
	end.	

run_index(#index_info{module=Module, function=Function}, Key, Value) ->
	try	apply(Module, Function, [Key, Value]) of
		IndexKeys -> {ok, IndexKeys}
	catch 
		Type:Error ->
			error_logger:error_msg("~p: Error calling function ~p:~p(~p, ~p) -> ~p:~p\n", [?MODULE, Module, Function, Key, Value, Type, Error]),
			{error, Error}
	end.

batch_exec(BRef, Cmds) ->
	eleveldb:write(BRef, Cmds, ?WRITE_OPTIONS).

put_data(Key, Value, Indexes, Cmds) ->
	KeyT = ?KEY_DATA(Key),
	ValueT = ?VALUE_DATA(Value, Indexes),
	make_put(KeyT, ValueT, Cmds).

put_index(Index, Key, ValueT, Cmds) ->
	KeyT = ?KEY_INDEX(Index, Key),
	make_put(KeyT, ValueT, Cmds).

make_put(Key, Value, Cmds) ->
	BinKey = term_to_binary(Key),
	BinValue = term_to_binary(Value),
	Cmd = {put, BinKey, BinValue},
	[Cmd|Cmds].

delete_data(Key, Cmds) ->
	KeyT = ?KEY_DATA(Key),
	make_delete(KeyT, Cmds).

delete_index(Index, Key, Cmds) ->
	KeyT = ?KEY_INDEX(Index, Key),
	make_delete(KeyT, Cmds).

make_delete(Key, Cmds) ->
	BinKey = term_to_binary(Key),
	Cmd = {delete, BinKey},
	[Cmd|Cmds].
