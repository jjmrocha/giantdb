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

-behaviour(gen_server).

-define(OPEN_OPTIONS, [{create_if_missing, true},
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

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/1]).
-export([open/1, close/1]).
-export([put/3, get/2, delete/2]).

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
	
% Data Management

-spec put(DBPid :: pid(), Key :: term(), Value :: term()) -> ok | {error, term()}.
put(DBPid, Key, Value) ->
	BinKey = convert(Key),
	BinValue = convert(Value),
	gen_server:call(DBPid, {put, BinKey, BinValue}).

-spec get(DBPid :: pid(), Key :: term()) -> {ok, term()} | not_found | {error, term()}.
get(DBPid, Key) ->
	BinKey = convert(Key),
	case gen_server:call(DBPid, {get, BinKey}) of
		{ok, BinValue} ->
			Value = binary_to_term(BinValue),
			{ok, Value};
		Other ->  Other
	end.

-spec delete(DBPid :: pid(), Key :: term()) -> ok | {error, term()}.
delete(DBPid, Key) ->
	BinKey = convert(Key),
	gen_server:call(DBPid, {delete, BinKey}).

%% ====================================================================
%% Behavioural functions 
%% ====================================================================
-record(state, {db_name, db_ref}).

%% init
init([DBName]) ->
	case eleveldb:open(DBName, ?OPEN_OPTIONS) of
		{ok, DBRef} ->
			error_logger:info_msg("~p: Database ~s was open by process ~p\n", [?MODULE, DBName, self()]),
			{ok, #state{db_name=DBName, db_ref=DBRef}};
		{error, Reason} ->
			error_logger:error_msg("~p: Error opening database ~s: ~p\n", [?MODULE, DBName, Reason]),
			{stop, Reason}
	end.

%% handle_call
handle_call({get, Key}, _From, State=#state{db_ref=DBRef}) ->
    Reply = eleveldb:get(DBRef, Key, ?READ_OPTIONS),
    {reply, Reply, State};

handle_call({put, Key, Value}, _From, State=#state{db_ref=DBRef}) ->
    Reply = eleveldb:put(DBRef, Key, Value, ?WRITE_OPTIONS),
    {reply, Reply, State};

handle_call({delete, Key}, _From, State=#state{db_ref=DBRef}) ->
    Reply = eleveldb:delete(DBRef, Key, ?WRITE_OPTIONS),
    {reply, Reply, State}.

%% handle_cast
handle_cast({close_db}, State) ->
	{stop, normal, State}.

%% handle_info
handle_info(Info, State) ->
	error_logger:info_msg("~p: handle_info(~p, ~p)\n", [?MODULE, Info, State]),
    {noreply, State}.

%% terminate
terminate(_Reason, #state{db_name=DBName, db_ref=DBRef}) ->
	eleveldb:close(DBRef),
	error_logger:info_msg("~p: Database ~s was closed by process ~p\n", [?MODULE, DBName, self()]),
	ok.

%% code_change
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

convert(Value) -> term_to_binary(Value).
