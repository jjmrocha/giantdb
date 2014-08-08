giantdb
=======
*GiantDB is a basic NoSQL (Key/Value type) database for Erlang clusters*


Installation
------------

Using rebar:

```erlang
{deps, [
	{giantdb, ".*", {git, "git://github.com/jjmrocha/giantdb", "master"}}
]}.
```


Configuration
-------------

Set the property "giantdb_dir" on your config file to define the localization of the GiantDB database (defaults to "./giantdb").

```erlang
[
{giantdb, [
	{giantdb_dir, "./giantdb"}
	]
}
].
```


Starting giantDB
----------------

If the Giant DB database is created or opened automatically when the application starts.


```erlang
ok = application:start(giantdb).
```


Bucket Management
-----------------

### Create bucket

```erlang
create_bucket(Bucket :: atom()) -> 
	ok | {error, Reason :: term()}.
```

Parameters:
* Bucket - **Bucket name**

### List buckets

```erlang
list_buckets() -> 
	{ok, BucketList :: list()} | {error, Reason :: term()}.
```

### Drop bucket

```erlang
drop_bucket(Bucket :: atom()) -> ok | {error, Reason :: term()}.
```

Parameters:
* Bucket - **Bucket name**

### Example

```erlang
1> application:start(giantdb).

=INFO REPORT==== 7-Aug-2014::15:51:59 ===
giantdb: Database ./giantdb was opened by process <0.39.0>
ok
2> giantdb:create_bucket(example).
ok
3> giantdb:list_buckets().        
{ok,[example]}
4> giantdb:drop_bucket(example).
ok
5> giantdb:list_buckets().      
{ok,[]}

```


Data Management
---------------

### Store Key/Value

```erlang
put(Bucket :: atom(), Key :: term(), Value :: term()) -> 
	ok | {error, Reason :: term()}.
```

Parameters:
* Bucket - **Bucket name**
* Key - **Key**
* Value - **Value**

### Retrieve Value

```erlang
get(Bucket :: atom(), Key :: term()) -> 
	{ok, Value :: term()} | not_found | {error, Reason :: term()}.
```

Parameters:
* Bucket - **Bucket name**
* Key - **Key**

### Delete Key

```erlang
delete(Bucket :: atom(), Key :: term()) -> 
	ok | {error, Reason :: term()}.
```

Parameters:
* Bucket - **Bucket name**
* Key - **Key**

### Example

```erlang
1> giantdb:put(example, 1, <<"Joaquim Rocha">>).
ok
2> giantdb:get(example, 1). 
{ok,<<"Joaquim Rocha">>}
3> giantdb:delete(example, 1).
ok
4> giantdb:get(example, 1).   
not_found
```


Index Management
----------------

### Create index

```erlang
create_index(Bucket :: atom(), Index :: atom(), Module :: atom(), Function :: atom()) -> 
	ok | {error, Reason :: term()}.
```

Parameters:
* Bucket - **Bucket name**
* Index - **Index name**
* Module - **Erlang module name where is the "Function"**
* Function - **Name of the function that generates the index values associated with the Key/Value pair**

*The function receive two parameters (Key and Value) and return a list of zero or more values.*

Function example:
```erlang
% Index player records using role field
index_role(_Key, #player{role=Role}) -> [Role];
index_role(_Key, _Value) -> [].
```

### List indexes

```erlang
list_indexes(Bucket :: atom()) -> 
	{ok, IndexList :: list()} | {error, Reason :: term()}.
```

Parameters:
* Bucket - **Bucket name**

### Drop index

```erlang
drop_index(Bucket :: atom(), Index :: atom()) -> 
	ok | {error, Reason :: term()}.
```

Parameters:
* Bucket - **Bucket name**
* Index - **Index name**

### Example

```erlang
1> giantdb:create_index(example, role, example, index_role).
ok
4> giantdb:list_indexes(example).
{ok,[{role,example,index_role}]}
2> giantdb:drop_index(example, role).
ok
3> giantdb:list_indexes(example).    
{ok,[]}
```


Search
------

### Find data using indexes

```erlang
find(Bucket :: atom(), Index :: atom(), IndexValue :: term()) -> 
	{ok, KeyValueList :: list()} | {error, Reason :: term()}.
```

Parameters:
* Bucket - **Bucket name**
* Index - **Index name**
* IndexValue - **Index value**

### Filter

```erlang
filter(Bucket :: atom(), Fun :: fun((Key :: term(), Value :: term()) -> boolean())) -> 
	{ok, KeyValueList :: list()} | {error, Reason :: term()}.
```

Parameters:
* Bucket - **Bucket name**
* Fun - **Function to filter Key/Values**

*The function receives two parameters (Key and Value) and must return a boolean.*

Function example:
```erlang
fun(Key, _Value) when is_integer(Key) -> Key >= 10 andalso Key =< 20;
   (_Key, _Value) -> false end.
```

### Using indexes

** List all index keys **

```erlang
index(Bucket :: atom(), Index :: atom()) -> 
	{ok, IndexValueList :: list()} | {error, Reason :: term()}.
```

Parameters:
* Bucket - **Bucket name**
* Index - **Index name**

** List all Key/Values pairs associated with one index value **

```erlang
index(Bucket :: atom(), Index :: atom(), Key :: term()) -> 
	{ok, IndexValueList :: list()} | {error, Reason :: term()}.
```

Parameters:
* Bucket - **Bucket name**
* Index - **Index name**
* IndexValue - **Index value**

### Iterate over all data

```erlang
foreach(Bucket :: atom(), Fun :: fun((Key :: term(), Value :: term()) -> any())) -> 
	ok | {error, Reason :: term()}.
```

Parameters:
* Bucket - **Bucket name**
* Fun - **Function to iterate over all Key/Value pairs**

*The function receives two parameters (Key and Value) the return is ignored.*

Function example:
```erlang
fun(Key, Value) ->
   io:format("{~p, ~p}~n", [Key, Value]) 
end.
```

### Example

```erlang
1> giantdb:put(example, jr, #player{name="Joaquim Rocha", role=fan}).
ok
2> giantdb:put(example, cr7, #player{name="Cristiano Ronaldo", role=player}).
ok
3> giantdb:put(example, number_1, #player{name="Jose Mourinho", role=manager}).   
ok
4> giantdb:find(example, role, fan).                        
{ok,[{jr,#player{name = "Joaquim Rocha",role = fan}}]}
5> Filter = fun(_, #player{role=player}) -> true;
5>             (_, #player{role=manager}) -> true;
5>             (_, _) -> false
5> end.
#Fun<erl_eval.12.90072148>
6> giantdb:filter(example, Filter).
{ok,[{number_1,#player{name = "Jose Mourinho",
                       role = manager}},
     {cr7,#player{name = "Cristiano Ronaldo",role = player}}]}
7> giantdb:index(example, role).
{ok,[manager,player,fan]}
8> giantdb:index(example, role, fan).
{ok,[jr]}
9> ListAll = fun(Key, Value) ->
9>    io:format("{~p, ~p}~n", [Key, Value])
9> end.
#Fun<erl_eval.12.90072148>
10> giantdb:foreach(example, ListAll).
{jr, {player,"Joaquim Rocha",fan}}
{cr7, {player,"Cristiano Ronaldo",player}}
{number_1, {player,"Jose Mourinho",manager}}
ok
```