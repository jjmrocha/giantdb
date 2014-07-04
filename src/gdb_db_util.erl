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

-module(gdb_db_util).

-include("giantdb.hrl").

-define(DB_CONFIG_FILENAME, "giantdb.conf").
-define(DB_CONFIG_DATA, [
		{?DB_CONFIG_CONFIG_PARAM, []}, 
		{?DB_CONFIG_BUCKETS_PARAM, []}
		]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([exists_db/1, create_db/1, open_db/1]).

-export([add_bucket/2, add_bucket/3, delete_bucket/2]).

-spec exists_db(DBDir :: string()) -> boolean() | {error, Reason :: any()}.
exists_db(DBDir) ->
	exists(DBDir).

-spec create_db(DBDir :: string()) -> {ok, DBInfo :: #db_info{}} | {error, Reason :: any()}.
create_db(DBDir) ->
	case exists(DBDir) of
		true -> {error, duplicated_db};
		false -> 
			case make_dir(DBDir) of
				ok ->
					ConfigFile = get_db_config_name(DBDir),
					case store_db_config(ConfigFile, ?DB_CONFIG_DATA) of
						ok ->
							DBInfo = #db_info{db_dir=DBDir, 
									config_file=ConfigFile, 
									db_config=?DB_CONFIG_DATA},							
							{ok, DBInfo};
						Error -> Error
					end;
				Error -> Error
			end;
		Error -> Error
	end.

-spec open_db(DBDir :: string()) -> {ok, DBInfo :: #db_info{}} | {error, Reason :: any()}.
open_db(DBDir) ->
	case exists(DBDir) of
		true -> 
			ConfigFile = get_db_config_name(DBDir),
			case read_db_config(ConfigFile) of
				{ok, DBConfig} ->
					DBInfo = #db_info{db_dir=DBDir, 
							config_file=ConfigFile, 
							db_config=DBConfig},							
					{ok, DBInfo};		
				Error -> Error
			end;
		false -> {error, no_db};
		Error -> Error
	end.

-spec add_bucket(DBInfo :: #db_info{}, Bucket :: string()) -> 
	{ok, BRef :: term(), DBInfo :: #db_info{}} | {error, Reason :: any()}.
add_bucket(DBInfo, Bucket) -> add_bucket(DBInfo, Bucket, []).

-spec add_bucket(DBInfo :: #db_info{}, Bucket :: string(), BucketConfig :: list()) -> 
	{ok, BRef :: term(), DBInfo :: #db_info{}} | {error, Reason :: any()}.
add_bucket(DBInfo, Bucket, BucketConfig) ->
	BucketDirName = get_bucket_name(DBInfo#db_info.db_dir, Bucket),
	case gdb_bucket_util:open_bucket(BucketDirName, BucketConfig) of
		{ok, BRef} ->
			{_, BucketList} = lists:keyfind(?DB_CONFIG_BUCKETS_PARAM, 1, DBInfo#db_info.db_config),
			BucketList1 = lists:keystore(Bucket, 1, BucketList, {Bucket, BucketDirName, BucketConfig}),
			DBConfig1 = lists:keystore(?DB_CONFIG_BUCKETS_PARAM, 1, DBInfo#db_info.db_config, {?DB_CONFIG_BUCKETS_PARAM, BucketList1}),			
			store_db_config(DBInfo#db_info.config_file, DBConfig1),
			{ok, BRef, DBInfo#db_info{db_config=DBConfig1}};
		Error -> Error
	end.

-spec delete_bucket(DBInfo :: #db_info{}, Bucket :: string()) -> 
	{ok, DBInfo :: #db_info{}} | {error, Reason :: any()}.
delete_bucket(DBInfo, Bucket) ->
	{_, BucketList} = lists:keyfind(?DB_CONFIG_BUCKETS_PARAM, 1, DBInfo#db_info.db_config),
	case lists:keyfind(Bucket, 1, BucketList) of
		{_, BucketDirName, _} ->
			BucketList1 = lists:keydelete(Bucket, 1, BucketList),
			case gdb_bucket_util:delete_bucket(BucketDirName) of
				ok ->
					DBConfig1 = lists:keystore(?DB_CONFIG_BUCKETS_PARAM, 1, DBInfo#db_info.db_config, {?DB_CONFIG_BUCKETS_PARAM, BucketList1}),
					store_db_config(DBInfo#db_info.config_file, DBConfig1),
					{ok, DBInfo#db_info{db_config=DBConfig1}};
				Error -> Error
			end;
		false -> {error, bucket_not_found}
	end.

%% ====================================================================
%% Internal functions
%% ====================================================================

make_dir(Dir) ->
	case exists(Dir) of
		true -> {error, duplicated_directory};
		false -> file:make_dir(Dir);
		Error -> Error
	end.

exists(Name) ->
	case file:read_file_info(Name) of
		{ok, _} -> true;
		{error, enoent} -> false;
		{error, Reason} -> {error, Reason}
	end.

get_db_config_name(DBDir) ->
	filename:join(DBDir, ?DB_CONFIG_FILENAME).

get_bucket_name(DBDir, Bucket) ->
	filename:join(DBDir, Bucket).

read_db_config(ConfigFile) ->
	file:script(ConfigFile).

store_db_config(ConfigFile, DBConfig) ->
	{ok, H} = file:open(ConfigFile, write),
	io:format(H, "~p.", [DBConfig]),
	file:close(H).