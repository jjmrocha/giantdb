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

-define(SERVER, {local, ?MODULE}).

-define(GIANTDB_TABLE, giantdb_table).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/0]).

-export([create_bucket/1, drop_bucket/1, list_buckets/0]).

-export([create_index/4]).

-export([get/2, put/3, delete/2]).

-export([filter/2, foreach/2]).

% Internal
start_link() ->
	gen_server:start_link(?SERVER, ?MODULE, [], []).

% Bucket Management

-spec create_bucket(Bucket :: atom()) -> ok | {error, term()}.
create_bucket(Bucket) ->
	gen_server:call(?MODULE, {create_bucket, Bucket}).

-spec drop_bucket(Bucket :: atom()) -> ok | {error, term()}.
drop_bucket(Bucket) ->
	gen_server:call(?MODULE, {drop_bucket, Bucket}).

-spec list_buckets() -> {ok, list()} | {error, term()}.
list_buckets() ->
	gen_server:call(?MODULE, {list_buckets}).

% Index Management

-spec create_index(Bucket :: atom(), Index :: atom(), Module :: atom(), Function :: atom()) -> ok | {error, term()}.
create_index(Bucket, Index, Module, Function) -> 
	gen_server:call(?MODULE, {create_index, Bucket, Index, Module, Function}).

% Data Management

-spec get(Bucket :: atom(), Key :: term()) -> {ok, term()} | not_found | {error, term()}.
get(Bucket, Key) ->
	case find_bucket(Bucket) of
		no_bucket -> {error, no_bucket};
		BInfo -> gdb_bucket_lib:get(BInfo, Key)
	end.

-spec put(Bucket :: atom(), Key :: term(), Value :: term()) -> ok | {error, term()}.
put(Bucket, Key, Value) ->
	case find_bucket(Bucket) of
		no_bucket -> {error, no_bucket};
		BInfo -> gdb_bucket_lib:put(BInfo, Key, Value)
	end.

-spec delete(Bucket :: atom(), Key :: term()) -> ok | {error, term()}.
delete(Bucket, Key) ->
	case find_bucket(Bucket) of
		no_bucket -> {error, no_bucket};
		BInfo -> gdb_bucket_lib:delete(BInfo, Key)
	end.

% Querys

-spec filter(Bucket :: atom(), Fun :: FilterFun) -> list() | {error, term()} 
		  when FilterFun :: fun(({Key :: term(), Value :: term()}) -> boolean()).
filter(Bucket, Fun) ->
	case find_bucket(Bucket) of
		no_bucket -> {error, no_bucket};
		BInfo -> gdb_bucket_lib:filter(BInfo, Fun)
	end	.	

-spec foreach(Bucket :: atom(), Fun :: AllFun) -> ok | {error, term()} 
		  when AllFun :: fun(({Key :: term(), Value :: term()}) -> any()).
foreach(Bucket, Fun) ->
	case find_bucket(Bucket) of
		no_bucket -> {error, no_bucket};
		BInfo -> gdb_bucket_lib:foreach(BInfo, Fun)
	end	.	

%% ====================================================================
%% Behavioural functions 
%% ====================================================================
-record(state, {db_info}).

%% init
init([]) ->
	process_flag(trap_exit, true),	
	{ok, DBDir} = application:get_env(giantdb, giantdb_dir),
	case gdb_db_lib:exists_db(DBDir) of
		true -> 
			case gdb_db_lib:open_db(DBDir) of
				{ok, DBInfo} -> 
					error_logger:info_msg("~p: Database ~s was opened by process ~p\n", [?MODULE, DBDir, self()]),
					create_table(),
					{_, BucketList} = lists:keyfind(?DB_CONFIG_BUCKETS_PARAM, 1, DBInfo#db_info.db_config),
					open_buckets(BucketList),
					{ok, #state{db_info=DBInfo}};
				{error, Reason} ->
					error_logger:error_msg("~p: Error opening database ~s: ~p\n", [?MODULE, DBDir, Reason]),
					{stop, Reason}
			end;
		false -> 
			case gdb_db_lib:create_db(DBDir) of
				{ok, DBInfo} -> 
					error_logger:info_msg("~p: Database ~s was created by process ~p\n", [?MODULE, DBDir, self()]),
					create_table(),
					{ok, #state{db_info=DBInfo}};
				{error, Reason} ->
					error_logger:error_msg("~p: Error creating database ~s: ~p\n", [?MODULE, DBDir, Reason]),
					{stop, Reason}
			end;			
		{error, Reason} ->
			error_logger:error_msg("~p: Error opening database ~s: ~p\n", [?MODULE, DBDir, Reason]),
			{stop, Reason}
	end.

%% handle_call

handle_call({list_buckets}, _From, State=#state{db_info=DBInfo}) ->
	{_, BucketList} = lists:keyfind(?DB_CONFIG_BUCKETS_PARAM, 1, DBInfo#db_info.db_config),
	List = lists:foldl(fun(?BUCKET_ROW(Bucket, _, _), Acc) ->
					[Bucket|Acc]
			end, [], BucketList),	
	Reply = {ok, List},
	{reply, Reply, State};

handle_call({create_index, Bucket, Index, Module, Function}, _From, State=#state{db_info=DBInfo}) ->
	{Reply, DBInfo2} = case exists(Bucket, DBInfo) of
		true -> 
			case find_bucket(Bucket) of
				no_bucket -> 
					Resp = {error, no_bucket},
					{Resp, DBInfo};
				BInfo -> 
					case gdb_db_lib:add_index(DBInfo, BInfo, Index, Module, Function) of
						{ok, BInfo1, DBInfo1} ->
							ets:insert(?GIANTDB_TABLE, BInfo1),
							{ok, DBInfo1};
						Error ->
							{Error, DBInfo}
					end
			end;			
		false ->
			Resp = {error, bucket_not_exists},
			{Resp, DBInfo}
	end,	
	{reply, Reply, State#state{db_info=DBInfo2}};

handle_call({create_bucket, Bucket}, _From, State=#state{db_info=DBInfo}) ->
	{Reply, DBInfo2} = case exists(Bucket, DBInfo) of
		true -> 
			Resp = {error, duplicated},
			{Resp, DBInfo};
		false ->
			case gdb_db_lib:add_bucket(DBInfo, Bucket) of
				{ok, BInfo, DBInfo1} ->
					ets:insert(?GIANTDB_TABLE, BInfo),
					{ok, DBInfo1};
				Error ->
					{Error, DBInfo}
			end
	end,
	{reply, Reply, State#state{db_info=DBInfo2}};

handle_call({drop_bucket, Bucket}, _From, State=#state{db_info=DBInfo}) ->
	{Reply, DBInfo2} = case exists(Bucket, DBInfo) of
		false -> 
			Resp = {error, bucket_not_exists},
			{Resp, DBInfo};
		true ->
			case find_bucket(Bucket) of
				no_bucket -> ok;
				BInfo ->
					gdb_bucket_lib:close_bucket(BInfo),
					ets:delete(?GIANTDB_TABLE, Bucket)
			end,
			case gdb_db_lib:delete_bucket(DBInfo, Bucket) of
				{ok, DBInfo1} ->
					{ok, DBInfo1};
				Error ->
					{Error, DBInfo}
			end
	end,
	{reply, Reply, State#state{db_info=DBInfo2}}.

%% handle_cast
handle_cast(Msg, State) ->
	error_logger:info_msg("~p: handle_cast(~p, ~p)\n", [?MODULE, Msg, State]),
	{noreply, State}.

%% handle_info
handle_info(Info, State) ->
	error_logger:info_msg("~p: handle_info(~p, ~p)\n", [?MODULE, Info, State]),
	{noreply, State}.

%% terminate
terminate(_Reason, _State) ->
	close_buckets(),
	drop_table(),
	ok.

%% code_change
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

create_table() ->
	Options = [set, public, named_table, {keypos, 2}, {read_concurrency, true}],
	ets:new(?GIANTDB_TABLE, Options).

drop_table() ->
	ets:delete(?GIANTDB_TABLE).

open_buckets(BucketList) ->
	lists:foreach(fun(?BUCKET_ROW(Bucket, BucketDirName, BucketConfig)) ->
				case gdb_bucket_lib:open_bucket(Bucket, BucketDirName, BucketConfig) of
					{ok, BInfo} -> ets:insert(?GIANTDB_TABLE, BInfo);
					{error, Reason} ->
						error_logger:error_msg("~p: Error opening bucket ~s: ~p\n", [?MODULE, Bucket, Reason])
				end
		end, BucketList).

close_buckets() ->
	List = ets:tab2list(?GIANTDB_TABLE),
	lists:foreach(fun(BInfo) ->
				gdb_bucket_lib:close_bucket(BInfo)
		end, List).

exists(Bucket, #db_info{db_config=Config}) ->
	{_, BucketList} = lists:keyfind(?DB_CONFIG_BUCKETS_PARAM, 1, Config),
	case lists:keyfind(Bucket, 1, BucketList) of
		false -> false;
		_ -> true
	end.

find_bucket(Bucket) ->
	case ets:lookup(?GIANTDB_TABLE, Bucket) of
		[] -> no_bucket;
		[BInfo] -> BInfo
	end.