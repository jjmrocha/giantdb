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

-module(giantdb).

-include("giantdb.hrl").

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/1]).

-export([open/1, close/1]).

-export([create_bucket/2, drop_bucket/2, list_buckets/1, get_bucket/2]).

-export([get/2, get/3, put/3, put/4, delete/2, delete/3]).

% Internal
start_link(DBName) ->
	gen_server:start_link(?MODULE, [DBName], []).

% DB Management

-spec open(DBName :: string()) -> {ok, pid()} | {error, term()}.
open(DBName) ->
	giantdb_sup:start_db_server(DBName).

-spec close(DBPid :: pid()) -> ok.
close(DBPid) ->
	gen_server:cast(DBPid, {close_db}).

% Bucket Management

-spec create_bucket(DBPid :: pid(), Bucket :: string()) -> ok | {error, term()}.
create_bucket(DBPid, Bucket) ->
	gen_server:call(DBPid, {create_bucket, Bucket}).

-spec drop_bucket(DBPid :: pid(), Bucket :: string()) -> ok | {error, term()}.
drop_bucket(DBPid, Bucket) ->
	gen_server:call(DBPid, {drop_bucket, Bucket}).

-spec list_buckets(DBPid :: pid()) -> {ok, list()} | {error, term()}.
list_buckets(DBPid) ->
	gen_server:call(DBPid, {list_buckets}).

-spec get_bucket(DBPid :: pid(), Bucket :: string()) -> {ok, binary()} | {error, term()}.
get_bucket(DBPid, Bucket) ->
	gen_server:call(DBPid, {get_bucket, Bucket}).

% Data Management

-spec get(BRef :: binary(), Key :: term()) -> {ok, term()} | not_found | {error, term()}.
get(BRef, Key) ->
	gdb_bucket_util:get(BRef, Key).

-spec get(DBPid :: pid(), Bucket :: string(), Key :: term()) -> {ok, term()} | not_found | {error, term()}.
get(DBPid, Bucket, Key) ->
	case get_bucket(DBPid, Bucket) of
		{ok, BRef} -> get(BRef, Key);
		Error -> Error
	end.

-spec put(BRef :: binary(), Key :: term(), Value :: term()) -> ok | {error, term()}.
put(BRef, Key, Value) ->
	gdb_bucket_util:put(BRef, Key, Value).

-spec put(DBPid :: pid(), Bucket :: string(), Key :: term(), Value :: term()) -> ok | {error, term()}.
put(DBPid, Bucket, Key, Value) ->
	case get_bucket(DBPid, Bucket) of
		{ok, BRef} -> put(BRef, Key, Value);
		Error -> Error
	end.

-spec delete(BRef :: binary(), Key :: term()) -> ok | {error, term()}.
delete(BRef, Key) ->
	gdb_bucket_util:delete(BRef, Key).

-spec delete(DBPid :: pid(), Bucket :: string(), Key :: term()) -> ok | {error, term()}.
delete(DBPid, Bucket, Key) ->
	case get_bucket(DBPid, Bucket) of
		{ok, BRef} -> delete(BRef, Key);
		Error -> Error
	end.

%% ====================================================================
%% Behavioural functions 
%% ====================================================================
-record(state, {db_name, db_info, buckets}).

%% init
init([DBName]) ->
	{ok, MasterDir} = application:get_env(giantdb, default_master_dir),
	case gdb_db_util:exists_db(DBName, MasterDir) of
		true -> 
			case gdb_db_util:open_db(DBName, MasterDir) of
				{ok, DBInfo} -> 
					error_logger:info_msg("~p: Database ~s was open by process ~p\n", [?MODULE, DBName, self()]),
					{_, BucketList} = lists:keyfind(?DB_CONFIG_BUCKETS_PARAM, 1, DBInfo#db_info.db_config),
					Buckets = open_buckets(BucketList),
					{ok, #state{db_name=DBName, db_info=DBInfo, buckets=Buckets}};
				{error, Reason} ->
					error_logger:error_msg("~p: Error opening database ~s: ~p\n", [?MODULE, DBName, Reason]),
					{stop, Reason}
			end;
		false -> 
			case gdb_db_util:create_db(DBName, MasterDir) of
				{ok, DBInfo} -> 
					error_logger:info_msg("~p: Database ~s was created by process ~p\n", [?MODULE, DBName, self()]),
					{ok, #state{db_name=DBName, db_info=DBInfo, buckets=dict:new()}};
				{error, Reason} ->
					error_logger:error_msg("~p: Error creating database ~s: ~p\n", [?MODULE, DBName, Reason]),
					{stop, Reason}
			end;			
		{error, Reason} ->
			error_logger:error_msg("~p: Error opening database ~s: ~p\n", [?MODULE, DBName, Reason]),
			{stop, Reason}
	end.

%% handle_call

handle_call({get_bucket, Bucket}, _From, State=#state{db_info=DBInfo, buckets=Buckets}) ->
	case dict:find(Bucket, Buckets) of
		{ok, BRef} ->
			Reply = {ok, BRef},
			{reply, Reply, State};
		false ->
			case find_bucket(Bucket, DBInfo) of
				false -> 
					Reply = {error, bucket_not_exists},
					{reply, Reply, State};		
				{_, BucketDirName, BucketConfig} ->
					case gdb_bucket_util:open_bucket(BucketDirName, BucketConfig) of
						{ok, BRef} -> 
							Buckets1 = dict:store(Bucket, BRef, Buckets),
							Reply = {ok, BRef},
							{reply, Reply, State#state{buckets=Buckets1}};
						{error, Reason} ->
							error_logger:error_msg("~p: Error opening bucket ~s: ~p\n", [?MODULE, Bucket, Reason]),
							Reply = {error, Reason},
							{reply, Reply, State}
					end					
			end
	end;

handle_call({list_buckets}, _From, State=#state{db_info=DBInfo}) ->
	{_, BucketList} = lists:keyfind(?DB_CONFIG_BUCKETS_PARAM, 1, DBInfo#db_info.db_config),
	List = lists:foldl(fun({Bucket, _, _}, Acc) ->
					[Bucket|Acc]
			end, [], BucketList),	
	Reply = {ok, List},
	{reply, Reply, State};

handle_call({create_bucket, Bucket}, _From, State=#state{db_info=DBInfo, buckets=Buckets}) ->
	{Reply, DBInfo2, Buckets2} = case exists(Bucket, DBInfo) of
		true -> 
			Resp = {error, duplicated},
			{Resp, DBInfo, Buckets};
		false ->
			case gdb_db_util:add_bucket(DBInfo, Bucket) of
				{ok, BRef, DBInfo1} ->
					Buckets1 = dict:store(Bucket, BRef, Buckets),
					{ok, DBInfo1, Buckets1};
				Error ->
					{Error, DBInfo, Buckets}
			end
	end,
	{reply, Reply, State#state{db_info=DBInfo2, buckets=Buckets2}};

handle_call({drop_bucket, Bucket}, _From, State=#state{db_info=DBInfo, buckets=Buckets}) ->
	{Reply, DBInfo2, Buckets2} = case exists(Bucket, DBInfo) of
		false -> 
			Resp = {error, bucket_not_exists},
			{Resp, DBInfo, Buckets};
		true ->
			Buckets1 = case dict:find(Bucket, Buckets) of
				false -> Buckets;
				{ok, BRef} ->
					gdb_bucket_util:close_bucket(BRef),
					dict:erase(Bucket, Buckets)
			end,
			case gdb_db_util:delete_bucket(DBInfo, Bucket) of
				{ok, DBInfo1} ->
					{ok, DBInfo1, Buckets1};
				Error ->
					{Error, DBInfo, Buckets1}
			end
	end,
	{reply, Reply, State#state{db_info=DBInfo2, buckets=Buckets2}}.

%% handle_cast
handle_cast({close_db}, State) ->
	{stop, normal, State}.

%% handle_info
handle_info(Info, State) ->
	error_logger:info_msg("~p: handle_info(~p, ~p)\n", [?MODULE, Info, State]),
	{noreply, State}.

%% terminate
terminate(_Reason, #state{db_name=DBName, buckets=Buckets}) ->
	close_buckets(Buckets),
	error_logger:info_msg("~p: Database ~s was closed by process ~p\n", [?MODULE, DBName, self()]),
	ok.

%% code_change
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

open_buckets(BucketList) ->
	lists:foldl(fun({Bucket, BucketDirName, BucketConfig}, Dict) ->
				case gdb_bucket_util:open_bucket(BucketDirName, BucketConfig) of
					{ok, BRef} -> dict:store(Bucket, BRef, Dict);
					{error, Reason} ->
						error_logger:error_msg("~p: Error opening bucket ~s: ~p\n", [?MODULE, Bucket, Reason]),
						Dict
				end
		end, dict:new(), BucketList).

close_buckets(Buckets) ->
	List = dict:to_list(Buckets),
	lists:foreach(fun({_, BRef}) ->
				gdb_bucket_util:close_bucket(BRef)
		end, List).

exists(Bucket, #db_info{db_config=Config}) ->
	{_, BucketList} = lists:keyfind(?DB_CONFIG_BUCKETS_PARAM, 1, Config),
	case lists:keyfind(Bucket, 1, BucketList) of
		false -> false;
		_ -> true
	end.

find_bucket(Bucket, #db_info{db_config=Config}) ->
	{_, BucketList} = lists:keyfind(?DB_CONFIG_BUCKETS_PARAM, 1, Config),
	lists:keyfind(Bucket, 1, BucketList).