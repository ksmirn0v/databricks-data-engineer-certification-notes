# Data Processing

Data processing modes:
- **batch processing**:
  the engine does not keep track of already processed data in the source
  (preferable for *Sylver*/*Gold* layer)
- **stream processing**:
  the engine keeps track of already processed data in the source
  (preferable for *Bronze* layer)

Generation of a data processing pipeline can be:
- **procedural**:
  a structured approach where 
  explicit steps are defined to manipulate data
- **declarative**: 
  instead of step-by-step instructions,
  the system determines the most efficient execution plan
  for the given transformation logic

## Table of Contents

> - [Data Ingestion](#data-ingestion)
>   - [Batch Ingestion](#batch-ingestion)
>     - [SQL Commands](#sql-commands)
>     - [DataFrame API](#dataframe-api)
>   - [Stream Ingestion](#stream-ingestion)
>     - [Reading the Stream](#reading-the-stream)
>     - [Writing the Stream](#writing-the-stream)
>       - [Trigger Intervals](#trigger-intervals)
>       - [For Each Batch](#for-each-batch)
>       - [States](#states)
>       - [Output Mode](#output-mode)
> - [Lakeflow Connect](#lakeflow-connect)
>   - [Managed Connectors](#managed-connectors)
>     - [Managed Query-Based Connectors](#managed-query-based-connectors)
>     - [Managed Streaming Connectors](#managed-streaming-connectors)
>     - [Managed Database Connectors](#managed-database-connectors)
>     - [Managed File Connectors](#managed-file-connectors)
>     - [Managed SaaS Connectors](#managed-saas-connectors)
>   - [Standard Connectors](#standard-connectors)
>     - [Standard Query-Based Connectors](#standard-query-based-connectors)
>     - [Standard Streaming Connectors](#standard-streaming-connectors)
>     - [Standard File Connectors](#standard-file-connectors)
>   - [Community Connectors](#community-connectors)
> - [Change Data Capture (CDC)](#change-data-capture-cdc)
> - [Schema Evolution Levels](#schema-evolution-levels)

---

## Data Ingestion

### Batch Ingestion

Read data and load it into a table in one shot.
Each run reprocesses the source.

#### SQL Commands

###### Reading the Data

_Read Files without Options_

Since it's a CTAS, schema is inferred automatically:
```
CREATE TABLE <table> AS SELECT * FROM <format>.`<location-path>`;
```

formats:
- self-describing (_e.g._ `json`, `parquet` _etc._)
- non self-describing (_e.g._ `csv`, `tsv` _etc._)

path:
- single file (_e.g._ `file1.json`)
- multiple files (_e.g._ `file*.json`)
- files in a path (_e.g._ `/path/to/dir`)

examples:
```
SELECT * FROM json.`file1.json`;
SELECT * FROM text.`/path/to/dir`;
SELECT * FROM binaryFile.`/path/to/dir`;
```

_Read files with Options_

Since it's a CTAS, schema is inferred automatically:
```
CREATE TABLE <table> AS SELECT * FROM read_files(
   '<location-path>',
   <key> => '<value>',
   ...
);
```

examples:
```
CREATE TABLE <table>
AS SELECT * FROM read_files(
    '<location-path>',
    format => 'csv',
    header => 'true',
    delimiter => ';'
);
```

_Referencing External Files_

Referencing external files creates an external table that's not a delta table:
```
CREATE TABLE <table>(<column-name> <data-type>, ...)
USING <format>
OPTONS (<key> = <value>, ...)
LOCATION '<location-path>';
```

examples:
```
CREATE TABLE <table>(<column-name> <data-type>, ...)
USING CSV
OPTONS (header = "true", delimiter = ";")
LOCATION '<location-path>';

CREATE TABLE <table>(<column-name> <data-type>, ...)
USING JDBC
OPTONS (url = "jdbc:sqlite://hostname:port", dbtable = "database.table", user = "username", password = "password");
```

converting to a delta table with a temporary view:
```
CREATE TEMP VIEW <temp-view>(<column-name> <data-type>, ...)
USING <format>
OPTONS (<key> = <value>, ...)
LOCATION '<location-path>';

CREATE TABLE <table> AS SELECT * FROM <temp-view>;
```

###### Writing the Data

_Create or Replace Table as Select (CRTAS)_

```
CREATE OR REPLACE TABLE <table>
AS SELECT * FROM <format>.`<path>`
```

_Insert Overwrites_

Can only work on existing tables with the matching schema.
```
INSERT OVERWRITE <table>
SELECT * FROM <format>.`<path>`
```

_Inserts_

Can only work when schemas match.

```
INSERT INTO <table>
SELECT * FROM <format>.`<path>`
```

_Upserts_

Schema evolutions are possible.

```
MERGE INTO <target-table> AS target
USING <source-table> AS source
ON target.<key> = source.<key> AND ...
WHEN MATCHED [AND <condition>] THEN
  UPDATE SET target.<column-name> = source.<column-name>, ...
WHEN NOT MATCHED [AND <condition>] THEN
  INSERT *
```

#### DataFrame API

[options](https://docs.databricks.com/aws/en/spark/api-options)

###### Reading the data

The programmatic (PySpark) way to read a batch source - `spark.read`:
```
df = (
  spark.read
    .format("<format>")
    .option("<key>", "<value>")
    .schema(<schema>)
    .load("<location-path>")
)
```

format:
- `delta` (default, shortcut - `spark.read.table(<catalog-name>.<schema-name>.<table-name>)`)
- `parquet` (shortcut - `spark.read.parquet(...)`)
- `json` (shortcut - `spark.read.json(...)`)
- `csv` (shortcut - `spark.read.csv(...)`)
- ...

options:
- common
  - `.option("ignoreCorruptFiles", <boolean>)`
  - `.option("ignoreMissingFiles", <boolean>)`
  - `.option("modifiedAfter", <optional[datetime]>)`
  - `.option("modifiedBefore", <optional[datetime]>)`
  - `.option("fileNamePattern", <optional[string]>)`
  - `.option("pathGlobFilter", <optional[string]>)`
  - ...
- CSV
  - `.option("badRecordsPath", <optional[string]>)`
  - `.option("columnNameOfCorruptRecord", <string>)`
  - `.option("header", <boolean>)`
  - `.option("delimiter", <string>)`
  - `.option("inferSchema", <boolean>)`
  - ...
- JSON
  - `.option("badRecordsPath", <optional[string]>)`
  - `.option("columnNameOfCorruptRecord", <string>)`
  - `.option("multiLine", <boolean>)`
  - ...

examples:
```
df = (
  spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ";")
    .load("<location-path>")
)
```

###### Writing the data

The programmatic (PySpark) way to write a batch DataFrame - `df.write`:
```
(
  df.write
    .format("<format>")
    .mode("<save-mode>")
    .option("<key>", "<value>")
    .partitionBy("<column-name>", ...)
    .saveAsTable("<catalog-name>.<schema-name>.<table-name>")
    #.save("<location-path>")
)
```

save mode:
- `append` - add rows to the target table/data
- `overwrite` - replace the target table/data
- `ignore` - do not do anything if the target table/data exists
- `errorifexists` - fail if the target table/data exists

save:
- `saveAsTable` - write to a table
- `save` - write to a path
- `insertInto` - write new data to a table by column position

options:
- common
  - `.option("mergeSchema", <boolean>)` - allow adding new columns on write
  - `.option("overwriteSchema", <boolean>)` - replace schema when `mode("overwrite")`
  - `.option("replaceWhere", <string>)` - selective overwrite of matching rows only

examples:
```
(
  df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_date")
    .saveAsTable("<catalog-name>.<schema-name>.<table-name>")
)
```

### Stream Ingestion

Apache Spark Structured Streaming is a near real-time processing engine,
offering end-to-end fault tolerance with exactly-once processing guarantees. 

A streaming workload reads an unbounded data from a source and writes to a sink incrementally.

Structured streaming can be done on views in Unity Catalog but only queried against Delta tables.

Structured Streaming only accepts append inputs and throws an exception 
if any modifications occur on the source.

Stream ingestion can be:
- Continuous - a data processing pipeline that runs without stopping
  and processes new data as it arrives
- Incremental/Triggered - a data processing pipeline that runs on schedule/trigger,
  processes all data that has arrived since the last run and stops

### Reading the Stream

_From a Delta Table_

```
(
    spark.readStream
        .option("<key>", "<value>")
        .table("<catalog-name>.<schema-name>.<table-name>")
)
```

If an input Delta Table doesn't have only append operations,
there are 4 options to handle these changes:
- `.option("skipChangeCommits", "true")` - 
  ignore transactions that modify/delete existing records
- full refresh - 
  remove a checkpoint and output table, restart the stream to reprocess all data
- `.option("readChangeFeed", "true")` - 
  instead of streaming the table's data, you stream its change feed
- materialized view -
  recomputes (fully or incrementally) and propagates changes automatically

_From a Generic Data Source_

```
(
    spark.readStream
        .format("<format>")
        .option("<key>", "<value>")
        .load("<location-path>")
)
```

options:
- common
  - `.option("cleanSource", <string[off|delete|archive]>)`
  - `.option("sourceArchiveDir", <optional[string]>)`
  - `.option("latestFirst", <boolean>)`
  - `.option("maxBytesPerTrigger", <optional[int]>)`
  - `.option("maxFilesPerTrigger", <optional[int]>)`
- Delta Table
  - `.option("skipChangeCommits", <boolean>)`
  - `.option("startingTimestamp", <optional[string]>)`
  - `.option("startingVersion", <optional[int]>)`
  - `.option("readChangeData", <boolean>)`

### Writing the Stream

Write the stream to a sink, providing a checkpoint location:
```
(
  df.writeStream
    .option("checkpointLocation", "<checkpoint-location-path>")
    .option("<key>", "<value>")
    .toTable("<table>")
)
```

Checkpoints track the information that identifies the query,
including state information and processed records.
Checkpoint directory contains:
- offsets
- commits
- state
- metadata

The checkpoint is what makes the stream restartable and gives exactly-once guarantee.

options:
- common
  - `.option("checkpointLocation", <string>)`
- Delta Table
  - `.option("mergeSchema", <boolean>)`

#### Trigger Intervals

Trigger intervals control how frequently Structured Streaming checks for new data:
```
(
  df.writeStream
    .trigger("<trigger-mode>")
    .option("checkpointLocation", "<checkpoint-location-path>")
    .toTable("<table>")
)
```

Trigger modes:
- not specified, default (processing time = 0, checking for new data every few milliseconds)
- `.trigger(processingTime='<string>')` (check the data with a defined period, _e.g._ `10 seconds`)
- `.trigger(once=True)` (process as much data as is available at the time streaming job starts as one single batch)
- `.trigger(availableNow=True)` (processes as much data as is available at the time streaming job starts as multiple micro-batches)
- `.trigger(realTime='<string>')`(stream processing runs continuously with ultra-low latency, _e.g._ `5 minutes`)

The options `maxFilesPerTrigger` and `maxBytesPerTrigger` can control the load on each micro-batch.

#### For Each Batch

`forEachBatch` is used to apply functions to the micro-batches,
derived from streaming sources:
```
(
  df.writeStream
    .option("checkpointLocation", "<checkpoint-location-path>")
    .forEachBatch(...)
)
```

Notes:
- `forEachBatch` provides only at-least-once write guarantees.
- `forEachBatch` doesn't work in continuous/real-time mode (use `forEach` instead).

#### States

Stateful Structured Streaming queries process data,
maintaining intermediate state across micro-batches.
Such queries include aggregations, duplicate removal, or stream-stream joins.

Watermarks control the threshold for when a query stops processing a state entity.
```
(
  df.withWatermark("event_time", "10 minutes")
    .groupBy(window("event_time", "5 minutes"), "id")
    .count()
)
```
- short watermarks have low tolerance for late data but low latency to produce final results
- long watermarks have high tolerance for late date but high latency to produce final results

###### State Store

To read a state store and changes:
```
df = (
  spark.read
    .format("statestore")
    .option("readChangeFeed", True)
    .option("changeStartBatchId", 2)
    .load("<checkpoint-location-path>")
)
```

###### State Metadata

To read a state metadata:
```
df = (
  spark.read
    .format("state-metadata")
    .load("<checkpoint-location-path>")
)
```

#### Output Mode

Only stateful streams, containing aggregations, require an output mode:
```
(
  df.writeStream
    .option("checkpointLocation", "<checkpoint-location-path>")
    .option("outputMode", "<output-mode>")
    .toTable("<table-name>")
)
```

Output Modes:
- `append` (operators only emit rows that don't change in future triggers)
- `update` (operators emit rows that changed during the trigger)
- `complete` (all resulting rows are emitted downstream)

---

## Lakeflow Connect

[Reference](https://docs.databricks.com/aws/en/ingestion/overview)

Lakeflow Connect offers simple and efficient connectors to ingest data from:
- local files
- popular enterprise applications
- databases
- cloud storage
- message buses
- ...

Lakeflow Connect uses incremental reads and writes to enable efficient ingestion.

### Managed Connectors

Managed Connectors define the ingestion from enterprise application and databases.

Under the hood managed connectors are declarative pipelines 
that ingest data to Streaming Delta Tables.
All managed connectors support pipeline creation using Databricks APIs and DABs.

#### Managed Query-Based Connectors

Connectors that query (with CTAS) a database directly on a schedule
and write to a streaming table.

Databases:
- Oracle
- Teradata
- SQL Server
- MariaDB
- MySQL
- PostgreSQL
- ...

#### Managed Streaming Connectors

Connectors that continuously read messages from a message bus or an event streaming source
and write to a streaming table.

Streaming sources:
- Kafka
- RabbitMQ
- ...

Connectors have the following components:
- Source
- Connection (authentication details for the streaming source)
- Ingestion Pipeline
- Destination Tables

#### Managed Database Connectors

These are fully managed connectors for ingesting data
using Change Data Capture (CDC).

Databases:
- MySQL
- PostgreSQL
- SQL Server
- ...

Connectors have the following components:
- Source
- Connection (authentication details for the database)
- Ingestion Gateway (data extraction)
- Staging Storage (temporary storage of the extracted data)
- Ingestion Pipeline
- Destination

#### Managed File Connectors

File Systems:
- Google Drive
- SharePoint
- ...

Connectors have the following components:
- Source
- Connection (authentication details for the storage service)
- Ingestion Pipeline
- Destination Tables

#### Managed SaaS Connectors

Connectors that stream data from enterprise applications.

Applications:
- SalesForce
- ...

Connectors have the following components:
- Source
- Connection (authentication details for the application)
- Ingestion Pipeline
- Destination Tables

### Standard Connectors

Standard connectors support several ingestion modes:
- batch (reprocess all records with each run, _e.g._ `spark.read.load()`)
- incremental batch (only new records are processed, _e.g._ Spark Structured Streaming in a triggered mode)
- streaming (continuously load micro batches of data at very short intervals, _e.g._ Spark Structured Streaming in a continuous mode)

#### Standard Query-Based Connectors

Ingestion from a database by querying the source directly (without CDC).

examples:
```
df = (
  spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://host:5432/<database-name>")
    .option("query", """
        SELECT * FROM <source-catalog-name>.<source-schema-name>.<source-table-name>
        WHERE ...
    """)
    .option("user", "<user>")
    .option("password", "<password>")   # use a secret, not a literal
    .load()
)

df.write.mode("overwrite").saveAsTable("<target-catalog-name>.<target-schema-name>.<target-table-name>)
```

#### Standard Streaming Connectors

Connectors that continuously read messages from a message bus or an event streaming source
and write to a streaming table.

examples:
```
import pyspark.sql.functions as F

(
  spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "<topic>")
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp")
    .writeStream
    .option("checkpointLocation", "<checkpoint-location-path>")
    .toTable("<catalog-name>.<schema-name>.<table-name>")
)
```

#### Standard File Connectors

There's a possibility to create connectors for:
- Google Drive
- SFTP servers
- SharePoint
- Files Stored on the Cloud
  - Auto Loader 
  (incremental and efficient processing of files as they arrive in cloud storage)
  - COPY INTO (load data from a file location to a Delta Tabe)

###### Auto Loader

[Schema Evolution](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema#how-does-auto-loader-schema-evolution-work)\
[Common Data Loading Patterns](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/patterns#filtering-directories-or-files-using-glob-patterns)

Auto Loader simplifies streaming files from the Cloud Storage.
It's possible to do file load without Auto Loader with (but it's less efficient).

As files are discovered, their metadata is persisted in a scalable key-value store (RocksDB)
in the checkpoint location of your Auto Loader pipeline.

_PySpark implementation_
```
(
  spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "<checkpoint-location-path>")
    .load("<location-path>")
    .writeStream
    .option("checkpointLocation", "<checkpoint-location-path>")
    .trigger(availableNow=True)
    .toTable("<catalog-name>.<schema-name>.<table-name>"))
```

_SQL implementation_
```
CREATE OR REFRESH STREAMING TABLE <catalog-name>.<schema-name>.<table-name>
AS SELECT * FROM STREAM read_files(
  '<location-path>',
  format => 'json'
);
```
(The SQL command creates a standalone streaming table
and, as a consequence, a Lakeflow pipeline: 
in this case the checkpointing is handled automatically)

To load to an external table in Unity Catalog, first register an external table and then
use the same mechanism to load the data into that table:
```
CREATE TABLE IF NOT EXISTS <catalog-name>.<schema-name>.<table-name>
USING DELTA
LOCATION '<location-path>';
```

To enable schema inference and evolution, use this option:
```
.option("cloudFiles.schemaLocation", "<checkpoint-location-path>")
```
For formats that don't encode data types Auto Loader encodes all columns as strings.

The schema evolution mode is controlled by `cloudFiles.schemaEvolutionMode`:
- `addNewColumns` (stream fails -> schema is updated -> restart stream; default if schema is provided)
- `addNewColumnsWithTypeWidening` (stream fails - schema is updated -> restart stream)
- `rescue` (stream doesn't fail; new columns are saved in the rescued data column)
- `failOnNewColumns` (stream fails -> waiting for manual update)
- `none` (stream doesn't fail; no schema evolution; default if schema is provided)

2 file detection modes:
- directory listing mode (detecting files by listing the input directory)
- file notification mode (subscribing to emitted file events in the input directory)
  - file events (file events come through a file events service)
  - classic (file events come via individual subscription)

Scheduling
- continuous
- file arrival trigger
- scheduled

Corrupted records can be stored in a column, specified by the option:
```
.option("columnNameOfCorruptRecord", "_corrupt_record")
```

Best practices:
- use an external volume instead of a direct cloud path to ingest files from

options:
- `.option("cloudFiles.format", <optional[string[avro|json|csv|...]]>)`
- `.option("cloudFiles.cleanSource". <string[OFF|DELETE|MOVE]>)`
- `.option("cloudFiles.cleanSource.retentionDuration", <string>)`
- `.option("cloudFiles.cleanSource.moveDestination", <optional[string]>)`
- `.option("cloudFiles.maxBytesPerTrigger", <optional[int]>)`
- `.option("cloudFiles.maxFilesPerTrigger", <optional[int]>)`
- `.option("cloudFiles.useNotifications", <boolean>)`

examples:
```
(
  spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("pathGlobfilter", "*.png") \
    .load("/Volumes/catalog_name/schema_name/volume_name/path")
)
```

```
(
  spark.readStream.format("cloudFiles")
    .schema(expected_schema)
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load("/Volumes/catalog_name/schema_name/volume_name/source_data")
    .writeStream
    .option("checkpointLocation", "<checkpoint-location-path>")
    .start("<target-location-path>")
)
```

```
(
  spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(<schema>)
    .load(<location-path>)
)
```

###### COPY INTO

AutoLoader is preferable over `COPY INTO` when:
- expecting files in the order of millions or more
- data schema is going to evolve frequently

It's possible to run `COPY INTO` repeatedly, 
and it will only load new data into your Delta table.

Load data in a schemaless Delta Table:
```
CREATE TABLE IF NOT EXISTS <catalog-name>.<schema-name>.<table-name>;

COPY INTO <catalog-name>.<schema-name>.<table-name>
FROM '/Volumes/<catalog-name>/<schema-name>/<volume-name>/<volume-path>'
FILEFORMAT = JSON
FORMAT_OPTIONS ('mergeSchema' = 'true', 'multiLine' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');
```

Set schema and load data into Delta Table:
```
CREATE TABLE <catalog-name>.<schema-name>.<table-name> (
  booking_id BIGINT,
  user_id BIGINT,
  status STRING,
  total_amount DOUBLE
);

COPY INTO <catalog-name>.<schema-name>.<table-name>
FROM '/Volumes/<catalog-name>/<schema-name>/<volume-name>/<volume-path>'
FILEFORMAT = JSON
FORMAT_OPTIONS ('multiLine' = 'true');
```

### Community Connectors

Community connectors are open-source connectors 
that extend Lakeflow Connect to sources without managed connector support.
The community builds and maintains them.

---

## Change Data Capture (CDC)

CDC treats a database as a set of changes, rather than as a complete static database.\
The challenge is that source systems provide data in different formats.

Table changes:
- SCD Type 1: Current state only (only the latest version of the data available)
- SCD Type 2: Complete history is preserved (the active records are marked)

Each CDC record from the source database includes:
- The operation type (`INSERT`, `UPDATE`, `DELETE`)
- The data values for the record
- A sequence number or timestamp for deterministic ordering

CDC for Delta Tables is called Change Data Feed (CDF).
To query the change data:
```
SELECT * FROM table_changes(<table>, start_version[, end_version]);
SELECT * FROM table_changes(<table>, start_timestamp[, end_timestamp]);
```

CDF can be enabled by setting a table property:
```
CREATE TABLE <table>(<column-name> <data-type>, ...) TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE <table> SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

CDC feeds can be processed with:
- AUTO CDC (when a source is set up to produce Change Data Capture feed)
- AUTO CDC FROM SNAPSHOT (when CDC feeds aren't available)

---

## Schema Evolution Levels

Schema evolution refers to a system's ability 
to adapt to changes in the structure of data over time:
1. new columns 
2. column renaming 
3. dropped columns 
4. type widening (_e.g._ *float* to *double*)
5. type changes

Schema changes can be handled on different levels:
- Connectors
  - Auto Loader (`cloudFiles.schemaEvolutionMode` and `rescuedDataColumn`; 1, 2, 3, 4 supported)
  - Delta Connector (1, 2, 3, 4 supported)
  - SaaS, CDC Connectors (queries automatically restart;  1, 2, 3 supported)
  - Kinesis, Kafka, Pub/Sub, Pulsar Connectors (not supported)
- Parsers
  - `from_json` (behaves like Auto Loader when automatic schema evolution is enabled)
  - `from_avro` (supported with Confluent Schema Registry)
  - `from_protobuf` (supported with Confluent Schema Registry)
  - `from_csv` (not supported)
  - `from_xml` (not supported)
- Engine
  - Structured Streaming (supported but requires manual change after failure)
- Datasets
  - Streaming Tables (1, 2, 3, 4 supported)
  - Materialized Views (full recompute triggered)
  - Delta Tables (supported)
  - Views (supported)

---

## File Metadata

A `_metadata` column is a hidden column that can be selected when ingesting files.
It contains:
- `file_path`
- `file_name`
- `file_size`
- `file_modification_time`
- `file_block_start`
- `file_block_length`

---

## Object Metadata

An `_object_metadata` column is a hidden column that can be selected when ingesting files.
It contains:
- `mime_type`
- `etag`
- `user_metadata`
- `system_metadata`
- `tags`

---