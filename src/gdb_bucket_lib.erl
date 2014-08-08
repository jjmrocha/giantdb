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

-export([make_index/4, remove_index/2]).

-export([index_key/3, index_get/3, index/2]).

-spec open_bucket(Bucket :: atom(), BucketPath :: string(), IndexList :: list()) -> {ok, BInfo :: #bucket_info{}} | {error, Reason :: term()}.
open_bucket(Bucket, BucketPath, IndexList) ->
	case eleveldb:open(BucketPath, ?OPEN_OPTIONS) of
		{ok, BRef} ->
			Indexes = lists:map(fun(?INDEX_ROW(Index, Module, Function)) -> 
							#index_info{
								index=Index, 
								module=Module, 
								function=Function} 
					end, IndexList),
			BInfo = #bucket_info{bucket=Bucket, ref=BRef, indexes=Indexes},
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
put(#bucket_info{ref=BRef, indexes=IndexList}, Key, Value) ->
	case get_old_indexes(BRef, Key) of
		{ok, OldIndexes} -> 
			case get_new_indexes(Key, Value, IndexList, []) of
				{ok, NewIndexes} ->
					Deleted = not_in(OldIndexes, NewIndexes, []),
					Added = not_in(NewIndexes, OldIndexes, []),
					Cmds = put_data(Key, Value, NewIndexes, []),
					case update_indexes_add_key(BRef, Added, Key, Cmds) of
						{ok, Cmds1} ->
							case update_indexes_delete_key(BRef, Deleted, Key, Cmds1) of
								{ok, Cmds2} -> batch_exec(BRef, Cmds2);
								Other -> Other
							end;
						Other -> Other
					end;
				Other -> Other
			end;
		Other ->  Other
	end.

-spec delete(BInfo :: #bucket_info{}, Key :: term()) -> ok | {error, term()}.
delete(#bucket_info{ref=BRef}, Key) ->
	case get_old_indexes(BRef, Key) of
		{ok, OldIndexes} -> 
			Cmds = delete_data(Key, []),
			case update_indexes_delete_key(BRef, OldIndexes, Key, Cmds) of
				{ok, Cmds1} -> batch_exec(BRef, Cmds1);
				Other -> Other
			end;			
		Other ->  Other
	end.			

-spec filter(BInfo :: #bucket_info{}, Fun :: FilterFun) -> list()
	when FilterFun :: fun((Key :: term(), Value :: term()) -> boolean()).
filter(#bucket_info{ref=BRef}, Fun) ->
	Fold = fun({BinKey, BinValue}, Acc) ->
			KeyT = binary_to_term(BinKey),
			case KeyT of
				?KEY_DATA(Key) ->
					?VALUE_DATA(Value, _) = binary_to_term(BinValue),
					case Fun(Key, Value) of
						true ->
							KeyPar = {Key, Value},
							[KeyPar|Acc];
						false -> Acc
					end;
				_ -> Acc
			end
	end,
	Data = eleveldb:fold(BRef, Fold, [], ?READ_OPTIONS),
	{ok, Data}.

-spec foreach(BInfo :: #bucket_info{}, Fun :: AllFun) -> ok
	when AllFun :: fun(({Key :: term(), Value :: term()}) -> any()).
foreach(#bucket_info{ref=BRef}, Fun) ->
	Fold = fun({BinKey, BinValue}, _Acc) ->
			KeyT = binary_to_term(BinKey),
			case KeyT of
				?KEY_DATA(Key) ->
					?VALUE_DATA(Value, _) = binary_to_term(BinValue),
					Fun(Key, Value),
					ok;
				_ -> ok
			end
	end,
	eleveldb:fold(BRef, Fold, ok, ?READ_OPTIONS).

-spec make_index(BInfo :: #bucket_info{}, Index :: atom(), Module :: atom(), Function :: atom()) -> 
	{ok, BInfo1 :: #bucket_info{}} | {error, term()}.
make_index(BInfo = #bucket_info{ref=BRef}, Index, Module, Function) ->
	IInfo = #index_info{index=Index, module=Module, function=Function},
	Fold = fun({BinKey, BinValue}, Acc) ->
			KeyT = binary_to_term(BinKey),
			case KeyT of
				?KEY_DATA(Key) ->
					?VALUE_DATA(Value, Indexes) = binary_to_term(BinValue),
					case run_index(IInfo, Key, Value) of
						{ok, IndexKeys} ->
							case load_index(BRef, Index, IndexKeys, []) of
								{ok, IndexKeyList} ->
									Indexes1 = lists:keystore(Index, 1, Indexes, {Index, IndexKeys}),
									Cmds = put_data(Key, Value, Indexes1, Acc),
									make_add_index_cmds(Key, Index, IndexKeyList, Cmds);
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
				IndexList = [IInfo | BInfo#bucket_info.indexes],
				{ok, BInfo#bucket_info{indexes=IndexList}};
			Other -> Other
		end
	catch _:Error -> {error, Error}
	end.

-spec remove_index(BInfo :: #bucket_info{}, Index :: atom()) -> 
	{ok, BInfo1 :: #bucket_info{}} | {error, term()}.
remove_index(BInfo = #bucket_info{ref=BRef}, Index) ->
	Fold = fun({BinKey, BinValue}, Acc) ->
			KeyT = binary_to_term(BinKey),
			case KeyT of
				?KEY_DATA(Key) ->
					?VALUE_DATA(Value, Indexes) = binary_to_term(BinValue),
					case lists:keyfind(Index, 1, Indexes) of
						false -> Acc;
						_ ->
							Indexes1 = lists:keydelete(Index, 1, Indexes),
							put_data(Key, Value, Indexes1, Acc)							
					end;
				?KEY_INDEX(Index, Key) -> delete_index(Index, Key, Acc);
				_ -> Acc
			end
	end,
	Cmds = eleveldb:fold(BRef, Fold, [], ?READ_OPTIONS),
	case batch_exec(BRef, Cmds) of
		ok ->
			IndexList = lists:filter(fun(#index_info{index=I}) when I =:= Index -> false;
						(_) -> true end, BInfo#bucket_info.indexes),
			{ok, BInfo#bucket_info{indexes=IndexList}};
		Other -> Other
	end.	

-spec index_key(BInfo :: #bucket_info{}, Index :: atom(), Key :: term()) -> 
	{ok, list()} | {error, term()}.
index_key(#bucket_info{ref=BRef}, Index, Key) ->
	IKey = ?KEY_INDEX(Index, Key),
	case read(BRef, IKey) of
		not_found -> {ok, []};
		{ok, List} -> {ok, List};
		Error -> Error
	end.

-spec index_get(BInfo :: #bucket_info{}, Index :: atom(), Key :: term()) -> 
	{ok, list()} | {error, term()}.
index_get(BInfo, Index, Key) ->
	case index_key(BInfo, Index, Key) of
		{ok, List} -> load_data(BInfo#bucket_info.ref, List, []);
		Other -> Other
	end.

-spec index(BInfo :: #bucket_info{}, Index :: atom()) -> {ok, list()} | {error, term()}.
index(#bucket_info{ref=BRef}, Index) ->
	Fold = fun({BinKey, _}, Acc) ->
			KeyT = binary_to_term(BinKey),
			case KeyT of
				?KEY_INDEX(Index, Key) -> [Key|Acc];
				_ -> Acc
			end
	end,
	Data = eleveldb:fold(BRef, Fold, [], ?READ_OPTIONS),
	{ok, Data}.

%% ====================================================================
%% Internal functions
%% ====================================================================

load_data(BRef, [Key|T], Output) ->
	case read(BRef, ?KEY_DATA(Key)) of
		{ok, ?VALUE_DATA(Value, _)} -> 
			KeyPar = {Key, Value},
			load_data(BRef, T, [KeyPar|Output]);
		Other ->  Other
	end;
load_data(_BRef, [], Output) -> {ok, Output}.

update_indexes_delete_key(BRef, [{Index, Keys}|T], Key, Cmds) ->
	case load_index(BRef, Index, Keys, []) of
		{ok, IndexKeyList} ->
			Cmds1 = make_remove_index_cmds(Key, Index, IndexKeyList, Cmds),
			update_indexes_delete_key(BRef, T, Key, Cmds1);
		Error -> Error
	end;
update_indexes_delete_key(_BRef, [], _Key, Cmds) -> {ok, Cmds}.

update_indexes_add_key(BRef, [{Index, Keys}|T], Key, Cmds) ->
	case load_index(BRef, Index, Keys, []) of
		{ok, IndexKeyList} ->
			Cmds1 = make_add_index_cmds(Key, Index, IndexKeyList, Cmds),
			update_indexes_add_key(BRef, T, Key, Cmds1);
		Error -> Error
	end;
update_indexes_add_key(_BRef, [], _Key, Cmds) -> {ok, Cmds}.

not_in([{Index, Keys}|T], Indexes, Output) ->
	case lists:keyfind(Index, 1, Indexes) of
		false -> 
			Output1 = lists:keystore(Index, 1, Output, {Index, Keys}),
			not_in(T, Indexes, Output1);
		{_, OtherKeys} ->
			NotIn = not_in(Keys, OtherKeys),
			case NotIn of
				[] -> not_in(T, Indexes, Output);
				_ ->
					Output1 = lists:keystore(Index, 1, Output, {Index, NotIn}),
					not_in(T, Indexes, Output1)
			end
	end;
not_in([], _Indexes, Output) -> Output.

not_in(Keys, OtherKeys) ->
	lists:foldl(fun(E, Acc) ->
				case lists:member(E, OtherKeys) of
					true -> Acc;
					false -> [E|Acc]
				end
		end, [], Keys).

make_remove_index_cmds(Key, Index, IndexKeyList, Cmds) ->
	lists:foldl(fun({IKey, IList}, Acc) ->
				case lists:member(Key, IList) of
					true -> 
						IList1 = lists:delete(Key, IList),
						case IList1 of
							[] -> delete_index(Index, IKey, Acc);
							_ -> put_index(Index, IKey, IList1, Acc)
						end;
					false -> Acc
				end
		end, Cmds, IndexKeyList).	

make_add_index_cmds(Key, Index, IndexKeyList, Cmds) ->
	lists:foldl(fun({IKey, IList}, Acc) ->
				case lists:member(Key, IList) of
					true -> Acc;
					false -> 
						IList1 = [Key|IList],
						put_index(Index, IKey, IList1, Acc)	
				end
		end, Cmds, IndexKeyList).	

load_index(BRef, Index, [Key|T], Output) ->
	IKey = ?KEY_INDEX(Index, Key),
	case read(BRef, IKey) of
		not_found -> 
			KeyPar = {Key, []},
			load_index(BRef, Index, T, [KeyPar|Output]);
		{ok, List} ->
			KeyPar = {Key, List},
			load_index(BRef, Index, T, [KeyPar|Output]);
		Error -> Error
	end;
load_index(_BRef, _Index, [], Output) -> {ok, Output}.

get_old_indexes(BRef, Key) ->
	case read(BRef, ?KEY_DATA(Key)) of
		{ok, ?VALUE_DATA(_, Indexes)} -> {ok, Indexes};
		not_found -> {ok, []};
		Other ->  Other
	end.

get_new_indexes(Key, Value, [H=#index_info{index=Index}|T], Output) ->
	case run_index(H, Key, Value) of
		{ok, IndexKeyList} ->
			Indexes = lists:keystore(Index, 1, Output, {Index, IndexKeyList}),
			get_new_indexes(Key, Value, T, Indexes);
		Error -> Error
	end;
get_new_indexes(_Key, _Value, [], Output) -> {ok, Output}.

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
