# Delta Lake

[Reference](https://docs.databricks.com/aws/en/delta/)
 
Delta Lake is an open-source storage framework/layer and the
default table format in Databricks 
(all tables are Delta unless stated otherwise).

## Table of Contents

> - [Delta Table](#delta-table)
>   - [Key Features](#key-features)
>   - [Transaction Log](#transaction-log)
> - [Create a Delta Table](#create-a-delta-table)
>   - [Standard Creation](#standard-creation)
>   - [Create an External Table](#create-an-external-table)
>   - [Create Table as Select (CTAS)](#create-table-as-select-ctas)
>   - [Generated Columns](#generated-columns)
>   - [Other Options](#other-options)
> - [Table Properties](#table-properties)
> - [Clone a Table](#clone-a-table)
>   - [Deep Clone](#deep-clone)
>   - [Shallow Clone](#shallow-clone)
> - [Constraints](#constraints)
>   - [Check Constraints](#check-constraints)
>   - [Not Null Constraints](#not-null-constraints)
>   - [Violations](#violations)
> - [Operations](#operations)
>   - [Append](#append)
>   - [Overwrite](#overwrite)
>     - [Full Overwrite](#full-overwrite)
>     - [Partial Overwrite](#partial-overwrite)
>   - [Delete](#delete)
>   - [Upsert](#upsert)
> - [Schema Evolution](#schema-evolution)
> - [Versioning](#versioning)
>   - [History](#history)
>   - [Selecting a Version](#selecting-a-version)
>   - [Restoring to a Version](#restoring-to-a-version)
> - [Optimizations](#optimizations)
>   - [Partitioning](#partitioning)
>   - [Optimize](#optimize)
>   - [Auto Optimize](#auto-optimize)
>     - [Optimized Writes](#optimized-writes)
>     - [Auto Compaction](#auto-compaction)
>   - [Z-Ordering](#z-ordering)
>   - [Liquid Clustering](#liquid-clustering)
>   - [Vacuuming](#vacuuming)
>   - [Predictive Optimization](#predictive-optimization)

---

## Delta Table

A Delta table on storage is simply a directory of Parquet data files
plus a `_delta_log/` subdirectory holding the transaction log.

### Key Features

It provides:
- data stored as **Parquet** files + a **JSON** transaction log
- ACID transactions on cloud object storage
- scalable metadata handling
- a full audit trail of all changes (time travel / history)
- schema enforcement (and optional schema evolution)
- the foundation on which a Lakehouse is built

### Transaction Log

The transaction log (DeltaLog, the `_delta_log/` directory) is the
ordered record of every transaction ever performed on the table -
the single source of truth.
 
- Each commit is written as a numbered JSON file
  (_e.g._ `00000000000000000000.json`, `00000000000000000001.json`, ...).
- Periodic Parquet checkpoints summarize the state for fast reads.
- It records the actions (add/remove files, metadata, protocol) that
  define the table's current version.

These properties enable:
- ACID guarantees
- optimistic concurrency control
- time travel (reading a table = replaying the log up to a version)

---

## Create a Delta Table

[Reference](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using)

### Standard Creation
```
CREATE TABLE [IF NOT EXISTS] <table>(<column-name> <data-type>, ...);
```

### Create an External Table
```
CREATE TABLE [IF NOT EXISTS] <table>(<column-name> <data-type>, ...)
LOCATION <location-path>;
```

### Create Table as Select (CTAS)
```
CREATE TABLE [IF NOT EXISTS] <target-table> AS SELECT * FROM <source-table>;
```

CTASs do not support schema declaration.

### Generated Columns

Columns whose values are automatically computed from other columns -
useful for deterministic partitioning/derivation.
```
CREATE TABLE [IF NOT EXISTS] <table> (
  event_timestamp TIMESTAMP,
  event_date DATE GENERATED ALWAYS AS (CAST(event_timestamp AS DATE)),
  ...
);
```

### Other Options

```
CREATE TABLE [IF NOT EXISTS] <table> ...
COMMENT <text>
PARTITIONED BY(<column-name>, ...)
CLUSTER BY(<column-name>, ...)
OPTIONS(<key> = <value>, ...)
TBLPROPERTIES(<key> = <value>, ...)
...;
```

---

## Table Properties

To set table properties on an existing table:
```
ALTER TABLE <table-name> SET TBLPROPERTIES ('delta.<property-name>' = <property-value>);
```

It's possible to set a table property using Spark Session configurations:
```
SET spark.databricks.delta.properties.defaults.<property-name> = <property-value>
```

Some important properties include:
- `appendOnly` - make a table append-only, forbid updating or deleting records
- `autoOptimize.optimizeWrite`
- `autoOptimize.autoCompact`
- `enableChangeDataFeed`
- `enableDeletionVectors`
- ...

---

## Clone a Table
 
### Deep Clone
 
full copy of data + metadata
```
CREATE TABLE <target-table>
DEEP CLONE <source-table>;
```
 
### Shallow Clone
 
copy of transaction logs (metadata only; data files stay in place)
```
CREATE TABLE <target-table>
SHALLOW CLONE <source-table>;
```

---

## Constraints
 
[Reference](https://docs.databricks.com/aws/en/tables/constraints)

Constraints guarantee that every row in the table satisfies them.

### Check Constraints
```
ALTER TABLE <table>
ADD CONSTRAINT <constaint-name> CHECK (<condition>);
```

Example:
```
ALTER TABLE <table>
ADD CONSTRAINT <constaint-name> CHECK (date > '2026-01-01');
```
 
### Not Null Constraints
```
ALTER TABLE <table>
ALTER COLUMN <column-name> SET NOT NULL;
```

### Violations

If an incoming row violates a `CHECK` or `NOT NULL` constraint, the
entire write fails and is rolled back.

Adding a constraint also validates the existing data: if any current
row would violate it, the `ADD CONSTRAINT` / `SET NOT NULL` statement
itself fails and the constraint is not added.

---

## Operations

### Append

Add new rows, leaving existing data untouched.

SQL:
```
INSERT INTO <table> VALUES (...), (...);
INSERT INTO <target-table> SELECT * FROM <source-table>;
```

Python:
```
df.write.mode("append").saveAsTable("<table>")
```

### Overwrite

#### Full Overwrite

Replace an entire table.

SQL:
```
INSERT OVERWRITE <target-table> SELECT * FROM <source-table>;
```

Python:
```
df.write.mode("overwrite").saveAsTable("<table>")
```

#### Partial Overwrite

Replace only the rows matching a predicate.

SQL:
```
INSERT INTO <target-table> REPLACE WHERE <condition>
SELECT * FROM <source-table>;
```

Python:
```
df.write.mode("overwrite").option("replaceWhere", "<condition>").saveAsTable("<table>")
```

### Delete

Remove rows matching a condition.

```
DELETE FROM <table> WHERE <condition>;
```

### Upsert

[Reference](https://docs.databricks.com/aws/en/sql/language-manual/delta-merge-into)

`MERGE INTO` performs an insert, update, and/or delete rows
in a target Delta table based on a match against a source.

Updating specific columns on match, otherwise inserting entire rows:
```
MERGE INTO <target-table> AS target
USING <source-table> AS source
ON target.<key> = source.<key> AND ...
WHEN MATCHED [AND <condition>] THEN
  UPDATE SET target.<column-name> = source.<column-name>, ...
WHEN NOT MATCHED [AND <condition>] THEN
  INSERT *
```

---

## Schema Evolution

Schema evolution refers to a system's ability
to adapt to changes in the structure of data over time:

Change types:
- new columns
- column renaming
- dropped columns
- type widening (_e.g._ float to double)
- type changes (_e.g._ string to float)

Delta Tables support schema evolution through `MERGE INTO`:
```
MERGE WITH SCHEMA EVOLUTION INTO <target-table> AS target
USING <source-table> AS source
ON target.<key> = source.<key> AND ...
WHEN MATCHED [AND <condition>] THEN
  UPDATE SET target.<column-name> = source.<column-name>, ...
WHEN NOT MATCHED [AND <condition>] THEN
  INSERT *
```

---

## Versioning

Every write creates a new table version,
so past states remain queryable.

### History

To get an entire history of a table, run this command:
```
DESCRIBE HISTORY <table>;
```

### Selecting a Version

By timestamp:
```
SELECT * FROM <table> TIMESTAMP AS OF 'YYYY-MM-DD';
```
 
By version
```
SELECT * FROM <table> VERSION AS OF <int>;
SELECT * FROM <table>@v<int>;
```

### Restoring to a Version
 
By timestamp:
```
RESTORE TABLE <table> TO TIMESTAMP AS OF 'YYYY-MM-DD';
```
 
By version:
```
RESTORE TABLE <table> TO VERSION AS OF <int>;
```

---

## Optimizations

[Optimizing Workloads](https://www.databricks.com/discover/pages/optimize-data-workloads-guide)\
[Adaptive Query Execution](https://docs.databricks.com/aws/en/optimizations/aqe)\
[Predictive Optimization](https://docs.databricks.com/aws/en/optimizations/predictive-optimization)\
[Liquid Clustering](https://docs.databricks.com/aws/en/tables/clustering#automatic-liquid-clustering)

### Partitioning

A partition is a subset of rows that share the same value for
predefined partitioning columns.

```
CREATE TABLE [IF NOT EXISTS] <table>(<column-name> <data-type>, ...)
PARTITIONED BY (<column-name>, ...);
```

Low cardinality fields should be used for partitioning. 
If most partitions < 1 Gb of data, the table is over-partitioned.

### Optimize

The `OPTIMIZE` command compacts many small files 
into fewer, larger ones (bin-packing), improving read performance.
```
OPTIMIZE <table>;
```

### Auto Optimize

**Auto Optimize** is a pair of Delta Lake features
that automatically keep file sizes healthy (128 Mb by default),
so you don't end up with the "small files problem".

#### Optimized Writes

Before writing, Spark adds a shuffle to rebalance the data,
so it produces fewer, larger files (targeting roughly 128 Mb),
instead of one small file per partition/task.

SQL:
```
ALTER TABLE <table> SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');
```

PySpark:
```
spark.conf.set('spark.databricks.delta.optimizeWrite.enabled', 'true')
```

#### Auto Compaction

After a write finishes, if the table has accumulated too many small files,
Databricks automatically runs a small compaction pass
that combines them into larger files (targeting roughly 128 Mb).

SQL:
```
ALTER TABLE <table> SET TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true');
```

PySpark:
```
spark.conf.set('spark.databricks.delta.autoOptimize.autoCompact', 'true')
```
 
### Z-Ordering

The `OPTIMIZE` command with Z-Ordering re-runs the entire clustering.
It doesn't have a "memory" of what's already clustered
(it's not an incremental operation).
```
OPTIMIZE <table> ZORDER BY id;
```

### Liquid Clustering

When creating a table, you can specify clustering keys.
```
CREATE TABLE <table>(<colnumn-name> <type>, ...) CLUSTER BY (<column-name>, ...);
```

Also, it's possible to add clustering keys to an existing table:
```
ALTER TABLE <table> CLUSTER BY (<column-name>, ...);
```
 
The `OPTIMIZE` command with Liquid Clustering adjusts already existing clusters.
 
If clustering keys are not known in advance,
it's possible to use Automatic Liquid Clustering:
```
CREATE TABLE <table>(<colname> <type>, ...) CLUSTER BY AUTO;
ALTER TABLE <table> CLUSTER BY AUTO;
```
The Automatic Liquid Clustering requires **Predictive Optimization**
enabled for Unity catalog managed tables.
 
Liquid clustering and partitioning/Z-ordering are mutually exclusive -
a table uses one or the other.
 
### Vacuuming
 
[Reference](https://docs.databricks.com/aws/en/sql/language-manual/delta-vacuum)
 
`VACUUM` removes data files no longer referenced by the table and older
than the retention threshold (default 7 days). It is what eventually
makes old time-travel versions unrecoverable.
```
VACUUM <table> RETAIN <int> <HOURS|DAYS>;
```
 
To reduce the retention period below the default value (7 days),
it's necessary to change the following setting:
```
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
```
 
### Predictive Optimization
 
Databricks automatically runs maintenance operations
(`OPTIMIZE`, `VACUUM`) on Unity Catalog managed tables,
so you don't have to schedule them manually.

---