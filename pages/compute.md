# Compute

Databricks compute refers to 
the selection of computing resources available on Databricks
to run data engineering, data science, and analytics workloads.

## Table of Contents

> - [Concepts](#concepts)
>   - [Instance Types](#instance-types)
>   - [Cluster](#cluster)
> - [Serverless Compute](#serverless-compute)
> - [Classic Compute](#classic-compute)
>   - [Create Compute](#create-compute)
>     - [Policy](#policy)
>     - [Access Mode](#access-mode)
>     - [Performance](#performance)
>     - [Other Options](#other-options)
>   - [Instance Pools](#instance-pools)
> - [SQL Warehouses](#sql-warehouses)

---

## Concepts

### Instance Types

| Family                  | Profile               | Good for                                                          |
|-------------------------|-----------------------|-------------------------------------------------------------------|
| General Purpose         | Balanced CPU / memory | Default for mixed ETL and interactive dev                         |
| Memory Optimized        | High RAM per core     | Wide joins/aggregations, large shuffles, caching, jobs that spill |
| Compute (CPU) Optimized | High CPU per GB RAM   | CPU-bound transforms, streaming, CPU ML inference                 |
| Storage Optimized       | Fast local NVMe/SSD   | Repeated reads with **disk cache**, analytics, ML                 |
| GPU Accelerated         | GPUs attached         | Deep learning / model training (use ML Runtime)                   |

There are on-demand and spot instance available.

**On-demand** - reliable, not reclaimed.
The driver is always on-demand.

**Spot** - cheaper but can be reclaimed by the cloud provider;
use for fault-tolerant workers with lax latency requirements.
Never use a spot instance (or a spot pool) for the driver.

### Cluster

A cluster consists of a driver node and _0...N_ worker nodes.
Driver and workers can use different instance types.

#### Driver node

Maintains cluster state and the SparkContext,
interprets the commands you run,
and coordinates the Spark executors. 
Notebooks attach to the driver. 
It is always on-demand.

#### Worker nodes

Run the Spark executors that do the distributed work.
At least one worker is needed to run Spark.

---

## Serverless Compute

On-demand automatically managed compute
that scales based on your workload requirements.\
The workspace must have a Unity Catalog enabled.

The code is run against a specific so-called environment version 
that managed and upgraded by Databricks.\
Serverless Compute doesn't support init scripts.
Instead, library installation is handled via serverless environments.

In notebooks, the timeout for a query execution is controlled by\
**Workspace Settings** -> **Compute** -> **Serverless interactive: Serverless interactive execution timeout**\
A timeout for an individual notebook can be set with `spark.databricks.execution.timeout`.

Serverless compute support only
continuous streaming and `Trigger.AvailableNow()`.\
(no `Trigger.ProcessingTime(interval)` or `Trigger.Continuous(interval)`)

Serverless compute doesn't support caching,
_e.g._ `df.cache()` or `df.persist()`

---

## Classic Compute

Provisioned compute resources that 
you create, configure, and manage for your workloads.\
They are deployed in the cloud provider account and not managed by Databricks.

Databricks provides 3 types of logging on a compute resource:
- Compute event logs (events that happened with the cluster, _e.g._ `CREATING`, `RUNNING` _etc._)
- Apache Spark driver and worker logs
  - Standard output
  - Standard error
  - Log4j logs
- Compute init-script logs

### Create Compute

#### Policy

A compute policy is a set of rules (JSON) 
that limits how users can configure compute when they create it.

- Attributes not named in a policy remain unrestricted.
- Only one limitation per attribute may apply.

A policy can control: 
- driver/worker node types
- autoscaling min/max
- DBR version
- auto-termination
- custom tags
- instance pools
- Spark config
- access/data-security mode
- _etc._

Example:
```
{
  "node_type_id": {
    "type": "allowlist",
    "values": ["m5.large", "m5.xlarge", "m5.2xlarge"],
    "defaultValue": "m5.xlarge"
  },
  "autoscale.max_workers": {
    "type": "range",
    "minValue": 1,
    "maxValue": 25,
    "defaultValue": 5
  },
  "autotermination_minutes": { "type": "fixed", "value": 30 }
}

```

There are 5 default policies available:
- **Personal Compute**:
  single-node, minimal options; individual interactive exploration
  (only available option for a new user unless workspace admins define otherwise)
- **Shared Compute**:
  multi-node all-purpose; collaborative exploration, analysis, ML
- **Power User Compute**:
  multi-node with ML Runtime; advanced data-science projects
- **Job Compute**:
  default compute for jobs; LTS runtime
- **Unrestricted**:
  compute with no limitations

#### Access Mode

There are 2 access modes:
- Standard - any user with `CAN ATTACH TO` permission can attach to the compute resource
- Dedicated - a specific user or a group can attach to the compute resource

#### Performance

You can create 2 types of clusters:
- **all-purpose** - good for collaborative interactive analysis.
  Can manually restart and terminate.
  Use the most recent version of a Databricks runtime version.
- **job** - dedicated cluster for a job/pipeline that starts 
  and terminates by the job scheduler.
  Use the LTS Databricks runtime version.

#### Other Options

- Worker Type
- Driver Type
- Enable Autoscaling
- Terminate after
- Tags

### Instance Pools

Databricks pools are a set of idle, ready-to-use instances.\
Databricks does not charge DBUs while instances are idle in the pool
(However, charges might apply from the cloud provider).\
If the pool does not have sufficient idle resources,
the pool expands by allocating new instances from the instance provider.

---

## SQL Warehouses

[SQL Warehouse Settings](https://docs.databricks.com/aws/en/compute/sql-warehouse/create#settings)\
[Create SQL Warehouse](https://docs.databricks.com/aws/en/compute/sql-warehouse/create)

Optimized compute resource for specific use cases
(_e.g._ a compute resource that lets you query and explore data on Databricks).
Can be classic or serverless.

There are 3 types:
- Serverless (compute runs on Databricks)
- Pro (compute runs on cloud provider account; allows connection to on-premises databases)
- Classic (compute runs on cloud provider account)

---