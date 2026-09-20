# Useful Information

## Table of Contents

> - [Apache Spark](#apache-spark)
>   - [Spark Properties](#spark-properties)
>   - [Special SQL Functions](#special-sql-functions)
>     - [JSON](#json)
>     - [Arrays](#arrays)
>     - [Structs](#structs)
>     - [Timestamps](#timestamps)
>     - [Strings](#strings)
>     - [Conditionals and Casts](#conditionals-and-casts)
>     - [Windows](#windows)
>     - [Pivot](#pivot)
>     - [Higher-Order Functions](#higher-order-functions)
>     - [User-Defined Functions](#user-defined-functions)
> - [SQL Hints](#sql-hints)
> - [Notebook Commands](#notebook-commands)
>   - [Magic Commands](#magic-commands)
>   - [dbutils](#dbutils)
>     - [File System](#file-system)
>     - [Widgets](#widgets)
>     - [Secrets](#secrets)
>     - [Notebook Workflow](#notebooks-workflow)
>     - [Task Values](#task-values)
> - [Command Line Interface (CLI)](#command-line-interface-cli)
>   - [Setup and Authentication](#setup-and-authentication)
>   - [File System](#file-system-1)
>   - [Jobs](#jobs)
> - [REST API](#rest-api)
>   - [Pipelines](#pipelines)
>   - [Jobs](#jobs-1)

---

## Apache Spark

Databricks provides 2 ways to execute queries:
- Spark Classic (transformations happen on the chosen machines)
- Spark Connect (transformations happen on the server via gRPC communication)

### Spark Properties

Configuring Spark properties can be achieved in:
- **Compute configuration**: 
  all notebooks and jobs within a compute resource
- **Notebook**: 
  only Spark session within a notebook

In a notebook, Spark properties can be configured:
- Python (`spark.conf.set("spark.sql.ansi.enabled", "true")`)
- SQL (`SET ANSI_MODE = false`)

To get a Spark property:
```
spark.conf.get("<property-name>")
```

Some relevant properties:
- `spark.sql.shuffle.partitions` - number of partitions for shuffles/joins (defaults to 200)
- `spark.sql.adaptive.enabled` - adaptive query execution
- `spark.sql.ansi.enabled` - ANSI SQL mode
- `spark.databricks.delta.retentionDurationCheck.enabled` - guard for `VACUUM` retention period
- `spark.databricks.streaming.realTimeMode.enabled` - enabling real-time processing
- `spark.databricks.execution.timeout` - a timeout for a notebook (serverless)

### Special SQL Functions

#### JSON

###### Parsing a JSON String

Given a column contains a JSON string, it's possible to get the nested fields by:
```
SELECT <json-column>:<field>:<subfield> FROM <table>;
SELECT get_json_object(<json-column>, '$.<field>.<subfield>') FROM <table>;
```

Another way is to use `from_json` but this solution requires that we provide a schema
(in this case the returned object is a struct):
```
SELECT from_json(<json-column>, <schema>).<field>.<subfield> FROM <table>;
```

To expand all the fields, we can use the `*` symbol:
```
SELECT from_json(<json-column>, <schema>).* FROM <table>;
```

###### Utilities

It's possible to infer a schema, stored in a sample JSON string:
```
schema_of_json(<json-string>)
```

To convert a column with a `struct` object to a JSON string:
```
to_json(<struct-column>)
```

#### Arrays

These are the most common functions, applied to arrays:
- `explode()` - unwrap an array, so each element gets in its own row
- `explode_outer()` - the same as `exmplode()` but keeps nulls and empty elements
- `posexplode()` - `explode()` with the element position
- `collect_set()` - aggregate rows into an array with unique elements (should be used with `GROUP BY`)
- `collect_list()` - aggregate rows into an array (should be used with `GROUP BY`)
- `array()` - build an array
- `size()` - length of an array
- `array_contains(<array-column>, <element)` - check if an `<array-column>` contains an element `<element>`
- `array_distinct()` - filter only unique elements of an array
- `sort_array()` - sort an array
- `slice()` - select a slice of an array
- `flatten()` - represent an array of arrays as a single array

#### Structs

To access a field in a struct, use the dot notation:
`<struct-column>.<field>`

To access all the fields in a struct, use the start method:
`<struct-column>.*`

To create a struct:
- from key-value pairs: `named_struct('a', 1, 'b', 2)`
- from existing columns: `struct(<column-name-1>, <column-name-2>, ...)`

#### Timestamps

These are the most common functions to work with timestamps:
- `current_date()`
- `current_timestamp()`
- `to_date(<string>)`
- `to_timestamp(<string>)`
- `date_add(<date>, n)`
- `date_sub(<date>, n)`
- `datediff(<date-2>, <date-1>)`
- `year()`
- `month()`
- `dayofweek()`

#### Strings

These are the most common functions to work with strings:
- `split(<string>, <pattern>)` - split a string into an array of strings
- `regexp_extract(<string>, <pattern>, idx)`
- `regexp_replace(<string>, <pattern>, <replacement>)`
- `trim()`
- `upper()`
- `lower()`

#### Conditionals and Casts

- `CASE WHEN <condition> THEN <value-1> ELSE <value-2> END`
- `CAST(<value> AS <data-type>)` - throws an error if cast is not possible
- `try_cast(<value> AS <data-type>)` - returns a null if cast is not possible
- `coalesce(<value-1>, <value-2>, ...)` - first non-null value

#### Windows

```
SELECT 
  *,
  <window-function> OVER (PARTITION BY <column-name-1> ORDER BY <column-name-2> DESC) AS <name>
FROM <table>;
```

Window functions:
- `ROW_NUMBER()`
- `RANK()`
- `DENSE_RANK()`
- `LAG(<column-name>, n)`
- `LEAD(<column-name>, n)`
- `SUM(<column-name>)`
- `AVG(<column-name>)`
- `COUNT(<column-name>)`

#### Pivot

Pivot:
```
SELECT * FROM <table>
PIVOT (SUM(<column-name-1>) FOR <column-name-2> IN ('<value-1>', '<value-2>', ...));
```

Unpivot:
```
SELECT * FROM <table>
UNPIVOT (<column-name-1> FOR <column-name-2> IN ('<value-1>', '<value-2>', ...));
```

#### Higher-Order Functions

###### FILTER

Filters elements in an array that follow a condition:
```
SELECT FILTER(<array-column>, x -> <condition>) AS <name> FROM <table>;
```

###### TRANSFORM

Transforms elements in an array:
```
SELECT TRANSFORM(<array-column>, x -> <transformation>) AS <name> FROM <table>;
```

###### EXISTS

Converts elements of an array to boolean flags:
```
SELECT EXISTS(<array-column>, x -> <condition>)
```

###### AGGREGATE

Performs a 'reduce' operation:
```
AGGREGATE(<array-column>, <initial-value>, (<cumulative-value>, <next-value>) -> ...)
```

###### ZIP_WITH

Combines two arrays element-wise:
```
zip_with(<array-column-1>, <array-column-2>, (<next-value-1>, <next-value-2>) -> ...)
```

#### User-Defined Functions

```
CREATE OR REPLACE FUNCTION <function>(<argument-name> <data-type>, ...) RETURNS <data-type>
RETURN <function-body>;

SELECT <function>(<argument-name>, ...) AS <name> FROM <table>;
```

---

## SQL Hints

Hints suggest specific approaches to generate an execution plan.

Syntax:
```
/*+ { partition_hint | join_hint | skew_hint } [, ...] */
```

Examples:
```
SELECT /*+ BROADCAST(<table-2>) */ * FROM <table-1> 
INNER JOIN <table-2> ON <table-1>.<column-name-1> = <table-2>.<column-name-2>;
```

---

## Notebook Commands

### Magic Commands

- `%python` - use Python language for a cell
- `%sql` - use SQL language for a cell
- `%scala` - use Scala language for a cell
- `%r` - use R language for a cell
- `%md` - render the cell as Markdown
- `%sh` - run a shell command on a driver node
- `%pip install <package>` - install a library into the notebook's session
- `%run <notebook-path>` - run another notebook inline (variables are shared)
- `%fs <command>` - file system operations

### dbutils

#### File System

- `dbutils.fs.ls("<path>")` - list files (returns a Python list)
- `dbutils.fs.cp("<source-path>", "<destination-path>", recurse=True)` - copy files
- `dbutils.fs.mv("<source-path>", "<destination-path>")` - move or rename files
- `dbutils.fs.rm("<path>", recurse=True)` - delete files/directories
- `dbutils.fs.mkdirs("<path>")` - create directories
- `dbutils.fs.head("<file-path>")` - read the first bytes of a file
- `dbutils.fs.put("<file-path>", "<text>", overwrite=True)` - write a string to a file

All these commands can be used with `%fs <command>`.

#### Widgets

- `dbutils.widgets.text("<parameter-name>", "<default-value>")` - create a widget with a free text
- `dbutils.widgets.dropdown("<parameter-name>", "<default-value>", ["value-1","value-2", ...])` - create a widget with a dropdown meny
- `dbutils.widgets.get("<parameter-name>")` - read a parameter value
- `dbutils.widgets.remove("<parameter-name>")` - remove a widget
- `dbutils.widgets.removeAll()` - remove all widgets

#### Secrets

- `dbutils.secrets.get(scope="<scope>", key="<key>")` - fetch a secret value
- `dbutils.secrets.listScopes()` - list available secret scopes

#### Notebooks Workflow

- `dbutils.notebook.run("<notebook-path>", <timeout>, {"<argument-name>":"<argument-value>", ...})` -
  run another notebook in a separate context; returns its exit value
- `dbutils.notebook.exit("<return-value>")` - stop the notebook and return a value to the caller

#### Task Values

- `dbutils.jobs.taskValues.set(key="<key>", value="<value>")` - publish a value for downstream tasks
- `dbutils.jobs.taskValues.get(taskKey="<task>", key="<key>")`  - read an upstream task's value

---

## Command Line Interface (CLI)

[Reference](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands#databricks-bundle-deploy)

### Setup and Authentication

- `databricks --version` - print the CLI version
- `databricks configure` - set up PAT (token) authentication
- `databricks auth login --host <workspace-url>` - set up OAuth authentication
- `databricks <command> --profile <profile-name>` - run a command using a named auth profile

### File System

- `databricks fs ls <path>` - list files
- `databricks fs cp <source-path> <destination-path> [--recursive]` - copy files
- `databricks fs mkdirs <path>` - create a directory
- `databricks fs rm <path> [--recursive]` - delete files/directories

### Jobs

- `databricks jobs create --json @<file-path>` - create a job from a JSON definition
- `databricks jobs get <job-id>` - show a job's configuration
- `databricks jobs run-now <job-id>` - trigger a job run immediately
- `databricks jobs delete <job-id>` - delete a job

---

## REST API

REST API requests are authenticated through:
- Personal Access Token (PAT) / Bearer Token
- OAuth token from a service principal

### Pipelines

- `POST /api/2.0/pipelines` - create a pipeline, return `pipeline_id`
- `GET /api/2.0/pipelines/<pipeline-id>` - get a pipeline definition
- `GET /api/2.0/pipelines/<pipline-id>/events` - event log entries of a pipeline

### Jobs

- `POST /api/2.2/jobs/create` - create a job, return `job_id`
- `POST /api/2.2/jobs/run-now` - trigger a job, return `run_id`
- `GET /api/2.2/jobs/list` - get a job list
- `GET /api/2.2/jobs/get?job_id=<job-id>` - get a job definition
- `GET /api/2.2/jobs/runs/get?run_id=<run-id>` - get a status of a run

---