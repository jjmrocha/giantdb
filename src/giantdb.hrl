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

-define(DB_CONFIG_CONFIG_PARAM, config).
-define(DB_CONFIG_BUCKETS_PARAM, buckets).

-record(db_info, {db_dir, config_file, db_config}).

-record(index_info, {index, module, function}).

-record(bucket_info, {bucket, ref, indexes=[]}).

-define(BUCKET_ROW(Bucket, BucketDirName, BucketConfig), {Bucket, BucketDirName, BucketConfig}).
-define(INDEX_ROW(Index, Module, Function), {Index, Module, Function}).