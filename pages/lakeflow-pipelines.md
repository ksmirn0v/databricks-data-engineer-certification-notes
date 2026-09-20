# Lakeflow Pipelines

Declarative Pipelines is a declarative framework for
building batch and streaming data pipelines in SQL and Python.

Benefits
- Automatic Orchestration
- Declarative Processing
- Incremental Processing
- Handles retries, checkpoints, and optimization

Pipelines add additional table properties 
and perform automatic maintenance using predictive optimization,
including `OPTIMIZE` and `VACUUM` operations.

When a pipeline update runs, it refreshes the materialized views and streaming tables:
- refresh
- full refresh
- reset streaming flow checkpoints (only streaming tables; clears checkpoints but not the data)

## Table of Contents

> - [Core Concepts](#core-concepts)
>   - [Datasets](#datasets)
>     - [Streaming Table](#streaming-table)
>     - [Materialized View](#materialized-view)
>     - [Temporary View](#temporary-view)
>   - [Flows](#flows)
>     - [Append Flow](#append-flow)
>     - [Update Flow](#update-flow)
>     - [Auto CDC Flow](#auto-cdc-flow)
>     - [Auto CDC from Snapshot Flow](#auto-cdc-from-snapshot-flow)
>   - [Sinks](#sinks)
>     - [Create Sink](#create-sink)
>     - [ForEachBatch Sink](#foreachbatch-sink)
>   - [Replace Where](#replace-where)
> - [Expectations](#expectations)
>   - [Single Expectations](#single-expectations)
>   - [Multiple Expectations](#multiple-expectations)
> - [Configs](#configs)
>   - [Pipeline Modes](#pipeline-modes)
>   - [Parameters](#parameters)
> - [Event Log](#event-log)

---

## Core Concepts

Pipelines run in 2 modes:
- triggered (the pipeline runs once and stops when all datasets are up to date)
- continuous (the pipeline runs indefinitely and processes new data as it arrives)

### Datasets

The persistent objects that a pipeline produces.

#### Streaming Table

A managed Delta table that is also a streaming target.
It's incremental and append-only.

It's defined with:
- declarative approach - `@dp.table()`
- imperative approach - `dp.create_streaming_table()`

###### Examples

_File Streaming_

PySpark:
```
@dp.table
def <table>():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .load("<location-path>")
  )
```

SQL:
```
CREATE OR REFRESH STREAMING TABLE <table>
  AS SELECT *
  FROM STREAM read_files(
    '<location-path>',
    format => "json"
  );
```

_Message Streaming_

PySpark:
```
@dp.table
def <table>():
  return (
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka_server:9092")
      .option("subscribe", "<topic-name>")
      .load()
  )
```
  
SQL:
```
CREATE OR REFRESH STREAMING TABLE <table>
  AS SELECT *
  FROM STREAM read_kafka(
    bootstrapServers => 'kafka_server:9092',
    subscribe => '<topic-name'
  );
```

#### Materialized View

A managed Delta table storing the precomputed result of a query,
refreshed as a batch target.
It's used for transformations and aggregations.

It's defined with:
- declarative approach - `@dp.materialized_view()`

###### Examples

PySpark:
```
@dp.materialized_view
def <target-table>():
  return (
    spark.read.table("<source-table>)
      .filter(<condition>)
      .groupBy(<group-column>)
      .agg(sum("<target-column>").alias("total_count"))
      .sort(desc("total_count"))
  )
```

SQL:
```
CREATE OR REFRESH MATERIALIZED VIEW <target-table>
  AS SELECT
    <group-column>,
    SUM(<target-column>) AS total_count
  FROM <soure-table>
  WHERE <condition>
  GROUP BY <group-column>
  ORDER BY total_count DESC;
```

#### Temporary View

A pipeline-scoped intermediate, not published to the catalog.
It's used to break a transformation into steps or share logic between datasets.

It's defined with:
- declarative approach - `@dp.temporary_view()` (or `@dp.view()`)

###### Examples

PySpark:
```
@dp.temporary_view
def table_deduplicated():
  return spark.read.table("<source-table>").dropDuplicates([<target-column>])
```

### Flows

Flows represent queries that move data into a target.

#### Append Flow

Appends into a streaming table (or a sink).
Fan-in: many append flows can target one table - a union without a full refresh.

It's defined with:
- declarative approach - `@dp.append_flow()`

###### Examples

PySpark
```
dp.create_streaming_table("<target-table>")

@dp.append_flow(target="<target-table>")
def <source-table-1>():
  return spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .load("<location-path-1>)

@dp.append_flow(target="<target-table>")
def <source-table-2>():
  return spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .load("<location-path-2>")

# Additional flows can be added without the full refresh that a UNION query would require:
@dp.append_flow(target="<target-table>")
def <source-table-3>():
  return spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .load("<location-path-3>")
```

#### Update Flow

A flow running in real-time mode (lowest latency), targeting a sink.
About the trigger, not fan-in.

It's defined with:
- declarative approach - `@dp.updated_flow()`

###### Examples

PySpark:
```
@dp.update_flow(
  name="<flow-name>",
  target="<target-table>",
  spark_conf={"pipelines.trigger": "RealTime"},
)
def <flow-name>():
  return spark.readStream.format("kafka").load()
```

#### Auto CDC Flow

Targets a streaming table.
It applies inserts/updates/deletes as SCD Type 1 or 2 from a source CDC feed.

To use the CDC APIs, your pipeline must:
- be a serverless Lakeflow pipeline
- be a Lakeflow pipeline Pro/Advanced edition

It's defined with:
- imperative approach - `dp.create_auto_cdc_flow()`

###### Examples

PySpark:
```
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

@dp.view
def <view-name>():
  return spark.readStream.table("<source-table>")

dp.create_streaming_table("<target-table>")

dp.create_auto_cdc_flow(
  target = "<target-table>",
  source = "<view-name>",
  keys = ["<key>"],
  sequence_by = col("<sequence-key>"),
  apply_as_deletes = expr("operation = 'DELETE'"),
  apply_as_truncates = expr("operation = 'TRUNCATE'"),
  except_column_list = ["operation", "<sequence-key>"],
  stored_as_scd_type = 1
)
```

SQL:
```
CREATE OR REFRESH STREAMING TABLE <target-table>;

CREATE FLOW <flow-name> AS AUTO CDC INTO
  <target-table>
FROM
  stream(<source-table>)
KEYS
  (<key>)
APPLY AS DELETE WHEN
  operation = "DELETE"
APPLY AS TRUNCATE WHEN
  operation = "TRUNCATE"
SEQUENCE BY
  <sequence-key>
COLUMNS * EXCEPT
  (operation, <sequence-key>)
STORED AS
  SCD TYPE 1;
```

#### Auto CDC from Snapshot Flow

It's the same as **Auto CDC Flow**
but for sources where only periodic snapshots are available.

To use the CDC APIs, your pipeline must:
- be a serverless Lakeflow pipeline
- be a Lakeflow pipeline Pro/Advanced edition

It's defined with:
- imperative approach - `dp.create_auto_cdc_from_snapshot_flow()`

###### Examples

PySpark:
```
from pyspark import pipelines as dp

@dp.view()
def <view-name>():
  return spark.read.table("<source-table>")
  
dp.create_streaming_table("<target-table>")

dp.create_auto_cdc_from_snapshot_flow(
  target = "<target-table>>",
  source = "<view-name>",
  keys = ["<key>"],
  stored_as_scd_type = 2
)  
```

### Sinks

External destinations a flow writes to, as an alternative to a dataset. 
Unlike a dataset (a managed table the pipeline owns and refreshes), 
a sink is an outside system the pipeline only writes to.

Destinations:
- Delta table
- Apache Kafka
- Azure Event Hub
- ...

#### Create Sink

Declares a destination.
You feed it with an append (or update) flow.

It's defined with:
- imperative approach - `dp.create_sink()`

###### Examples

PySpark
```
dp.create_sink(
  name = "<target-sink>",
  format = "delta",
  options = { "tableName": "<catalog-name>.<schema-name>.<table-name>" }
)

@dp.append_flow(name = "<flow-name>", target="<target-sink>")
def <flow-name>():
    return(
        spark.readStream.table("<source-table>")
        .selectExpr(
            "<column-name-1>",
            "<column-name-w>",
            ...
        )
    )
```

#### ForEachBatch Sink

A custom sink running arbitrary `foreachBatch(df, batch_id)` logic per micro-batch -
use when no built-in sink format fits, _e.g._
- merges
- fan-out to multiple destinations
- external systems

it's defined with:
- declarative approach - `@dp.foreach_batch_sink()`

formats
- delta (a Delta table or path)
- kafka
- Azure Event Hub
- ...

###### Examples

PySpark:
```
from pyspark import pipelines as dp

# Create a ForEachBatch sink
@dp.foreach_batch_sink(name = "<target-sink>")
def <target-sink>(df, batch_id):
  # Custom logic here. You can perform merges,
  # write to multiple destinations, etc.
  return

# Create source data for example:
@dp.table()
def <source-table>():
  return spark.range(5)

# Add sink to an append flow:
@dp.append_flow(target="<target-sink>")
def <flow-name>():
  return spark.readStream.format("delta").table("<source-table>")
```

### Replace Where

recompute and overwrite a targeted subset of a table
without reprocessing your entire table history.

The 'Replace Where' is an incremental batch flow, without streaming semantics
(But it still can produce streaming tables).

###### Examples

PySpark:
```
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col

@dp.table(
  replace_where=col("date") >= F.date_sub(F.current_date(), 7)
)
def <target-table>():
  source_table_1 = spark.read.table("<source-table-1>")
  source_table_2 = spark.read.table("<source-table-2")
  return source_table_1.join(source_table_2, "<join-column-name>")
```
   
SQL:
```
CREATE STREAMING TABLE <target-table>
FLOW REPLACE WHERE date >= date_add(current_date(), -7) BY NAME
SELECT
  *
FROM <source-table-1> st1
JOIN <source-table-2> st2
  ON st1.<join-column-name> = st2.<join-column-name>;
```

---

## Expectations

[Reference](https://docs.databricks.com/aws/en/ldp/expectations)

optional clauses in pipeline materialized view, streaming table, or view creation statements 
that apply data quality checks on each record passing through a query.

### Single Expectations

PySpark:
```
@dp.table
@dp.expect("valid_customer_age", "age BETWEEN 0 AND 120")
def <target-table>():
  return spark.readStream.table("<source-table>)
```

SQL:
```  
CREATE OR REFRESH STREAMING TABLE <target-table>(
  CONSTRAINT valid_customer_age EXPECT (age BETWEEN 0 AND 120)
) AS SELECT * FROM STREAM(<source-table>);
```

Actions when an expectation fails:
- `warn` (`EXPECT`, default - invalid records are written to the target)
- `drop` (`EXPECT ... ON VIOLATION DROP ROW`)
- `fail` (`EXPECT ... ON VIOLATION FAIL UPDATE`)

### Multiple Expectations

PySpark:
```
valid_pages = {
    "valid_count": "count > 0",
    "valid_current_page": "current_page_id IS NOT NULL AND current_page_title IS NOT NULL"
}

@dp.table
@dp.expect_all(valid_pages)
def <target-table>():
  # Create a raw dataset

@dp.table
@dp.expect_all_or_drop(valid_pages)
def <target-table>():
  # Create a cleaned and prepared dataset

@dp.table
@dp.expect_all_or_fail(valid_pages)
def <target-table>():
  # Create cleaned and prepared to share the dataset
```

---

## Configs

### Pipeline Modes

Pipeline modes:
- triggered (the system stops after refreshing all tables based on the data available)
- continuous (the system processes new data as it arrives in data sources)
  - `spark.databricks.streaming.realTimeMode.enabled` allows the real-time mode\
    (real-time mode requires an update flow - `db.update_flow()`)
  - `pipelines.trigger.interval` controls the trigger for a flow to update table/entire pipeline

### Parameters

Pipeline parameters are key-value pairs.

JSON:
```
{
  "name": "<pipeline-name>",
  "parameters": {
    "source_catalog": "dev_catalog",
    "source_schema": "sales",
    "start_date": "2026-01-01"
  }
}
```

YAML:
```
resources:
  pipelines:
    my_pipeline:
      name: <pipeline-name>
      parameters:
        source_catalog: dev_catalog
        source_schema: sales
        start_date: '2026-01-01'
```

---

## Event Log

The pipeline event log contains all information related to a pipeline, including:
- audit logs
- data quality checks
- pipeline progress
- data lineage
- ...

To query an event log:
```
SELECT * FROM event_log(<pipeline-id>);
```

Better to create a view over the event log:
```
CREATE VIEW <view-name>
AS SELECT * FROM event_log(<pipeline-id>);
```

Schema:
- `id`
- `sequence`
- `origin`
- `timestamp`
- `level`
- `maturity_level`
- `error`
- `details`
- `event_type`

You can set up event hooks on 
events getting into a pipelines' event log:
```
@dp.on_event_hook(max_allowable_consecutive_failures=None)
def user_event_hook(event):
  # Python code defining the event hook
  
@dp.on_event_hook
def my_event_hook(event):
  if (
    event['event_type'] == 'update_progress' and
    event['details']['update_progress']['state'] == 'STOPPING'
  ):
    print('Received notification that update is stopping: ', event)
```

---