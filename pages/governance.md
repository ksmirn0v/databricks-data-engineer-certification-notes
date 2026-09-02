# Governance

## Table of Contents

> - [Identities](#identities)
> - [Administration](#administration)
>   - [Account](#account)
>   - [Workspace](#workspace)
>   - [Group Manager](#group-manager)
>   - [Service Principal Manager](#service-principal-manager)
> - [Unity Catalog Privileges](#unity-catalog-privileges)
> - [Workspace Access Level Control](#workspace-access-level-control)
>   - [Compute Policies](#compute-policies)
>   - [Compute Resources](#compute-resources)
>     - [Clusters](#clusters)
>     - [Instance Pools](#instance-pools)
>     - [SQL Warehouses](#sql-warehouses)
>   - [Workspace Objects and Files](#workspace-objects-and-files)
>   - [Pipelines](#pipelines)
>   - [Jobs](#jobs)
> - [Fine-Grained Access Control](#fine-grained-access-control)
>   - [Dynamic Views](#dynamic-views)
>     - [Masking a column](#masking-a-column)
>     - [Filtering rows](#filtering-rows)
>   - [ABAC Policies](#abac-policies)
> - [System Tables](#system-tables)

---

## Identities

There are 3 identities in Databricks:
- Users (represented by email addresses)
- Service Principals (used with jobs, automated tools, CI/CD platforms)
- Groups

To manage identities you should have one of the following roles:
- Account admins
- Workspace admins
- Groups managers
- Service principal managers

---

## Administration

[Authentication](https://docs.databricks.com/aws/en/dev-tools/auth/pat)

### Account

Account administration is done by users with the **Account admin** role.

The creator of an account automatically is assigned
the **Account owner** role. 
Technically, it's the same as **Account admin**
but it can't be removed.

The **Account admin** role can be assigned
to users/service principals by other users/service principals
with the **Account admin** role.
The same applies to role removal (including self-removal).
(with the exception of **Account owner**).

**Account admin** abilities:
- has access to the account console
- can create workspaces
- can enable Unity Catalog (creating a Unity Catalog metastore) for a workspace
- can create users, groups, service principals and assign workspaces to them
- can enable system tables to access audit/usage logs, data lineage
- can manage subscriptions
- can delegate account/workspace admin roles to any other user

### Workspace

Workspace administration is done by users
that are members of the system `admins` group.

The workspace creator becomes a Workspace admin automatically.

An Account/Workspace admin can
add/remove any other user/service principal to the `admins` group.

**Workspace admin** abilities:
- has admin privileges over a workspace
- can assign admin workspace role to a user/service principal
- has access to the admin settings page in a workspace
- can assign users/groups/service principals to a workspace
- can create SQL warehouses and clusters in a workspace
- can regulate how the compute resources are created/used in a workspace
- can manage workspace features and settings

### Group Manager

A Group Manager (user, group, or service principal) manages a specific group.
It's not necessary it is a group member.

Responsibilities:
- manage the group's membership (add/remove)
- delete the group
- grant `Manage`permissions to other group members
- grant `Assume` on the group to someone
  (_e.g._ a temporary substitution of a role for a given session)

### Service Principal Manager

A Service Principal Manager (user, group, or service principal) manages a specific service principal.
The creator of a service principal automatically becomes its Service Principal Manager.

Responsibilities:
- manage the entitlements of a service principal
- grant/revoke roles on the service principal
  (including appointing other managers and users)
- generate OAuth secrets for it

---

## Unity Catalog Privileges

[Reference](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges)

These privileges govern data objects browsable in the Catalog Explorer:
- `CATALOG`
- `SCHEMA`
- `TABLE`
- `VIEW`
- `VOLUME`
- `FUNCTION`
- `MODEL`
- `EXTERNAL LOCATION`
- `STORAGE CREDENTIAL`
- `CONNECTION`
- `SHARE`

Governing a privilege can be done programmatically via SQL:
```
GRANT/DENY/REVOKE <privilege> ON <object-type> <object-name> TO <user/group/service principal>
```

To govern privileges, you should be:
- Workspace admin
- Object owner

Common privileges:
- `USE CATALOG` - traverse into a catalog (not access to its objects by itself)
- `USE SCHEMA` - traverse into a schema (not access to its objects by itself)
- `SELECT` - read a table/view
- `MODIFY` - add/update/delete data (`INSERT`/`UPDATE`/`DELETE`)
- `CREATE` - `CREATE SCHEMA`, `CREATE TABLE`, `CREATE VOLUME`, `CREATE FUNCTION`, ...
- `EXECUTE` - invoke a function or load a registered model
- `READ VOLUME` - read files in a volume
- `WRITE VOLUME` - write files in a volume
- `READ FILES` - read / write files in an external location
- `WRITE FILES` - write files in an external location
- `CREATE EXTERNAL TABLE` - create a table in an external location
- `CREATE EXTERNAL VOLUME` - create a volume in an external location
- `APPLY TAG` - add/edit tags
- `BROWSE` - discover an object's metadata without `USE CATALOG`
- `MANAGE` - manage privileges, transfer ownership, rename, drop, `ALTER`
- `ALL PRIVILEGES` - all applicable privileges for the object type
  (note: **does not** include `MANAGE`)

To show the privilege list for a given object:
```
SHOW GRANTS <user/group/service principal> ON <object-type> <object-name>
```

---

## Workspace Access Level Control

Workspace ACLs control permissions over non-data objects.

They are set via:
- Permissions dialog in the UI
- Permissions API

### Compute Policies

Controls who may use a policy to create compute.

Permissions:
- `CAN USE` - create compute governed by this policy
- `CAN MANAGE` - edit the policy and its permissions

Workspace admins have access to all the available policies and `CAN MANAGE` them.
They can grant `CAN USE` permissions to other users.

### Compute Resources

#### Clusters

Independently if it's a `all-purpose-cluster` or a `job-cluster`
the following permissions can be applied:
- `NO PERMISSIONS`
- `CAN ATTACH TO` - attach a notebook, view logs/Spark UI
- `CAN RESTART` - start/restart/terminate (implies `CAN ATTACH TO`)
- `CAN MANAGE` - edit configuration, manage permissions (implies `CAN RESTART`)

The creator of a cluster automatically gets a `CAN MANAGE` permission.

#### Instance Pools

The following permissions can be applied:
- `CAN ATTACH TO` - use the pool for compute
- `CAN MANAGE` - edit and manage permissions

The creator of an instance pool automatically gets a `CAN MANAGE` permission.

#### SQL Warehouses

The following permissions can be applied:
- `CAN VIEW` - see the warehouse and its query history
- `CAN MONITOR` - monitor its queries
- `CAN USE` - run queries on it
- `IS OWNER` - the owner
- `CAN MANAGE` - full control: edit configuration, start/stop, and manage permissions

The creator of an SQL Warehouse automatically gets `IS OWNER` and `CAN MANAGE` permissions

### Workspace Objects and Files

Notebooks, folders/directories, and workspace files (.py, .sql, .txt, etc.) share one ACL. 
Folder permissions inherit to their contents.

The following permissions can be applied:
- `NO PERMISSIONS`
- `CAN READ` (or `CAN VIEW`) - view and comment
- `CAN RUN` - run (and attach/execute)
- `CAN EDIT` - modify
- `CAN MANAGE` - full control including permissions

The creator of a file automatically gets a `CAN MANAGE` permission.

### Pipelines

The following permissions can be applied:
- `CAN VIEW`
- `CAN RUN`
- `IS OWNER`
- `CAN MANAGE`

The creator of a pipeline automatically gets a `IS OWNER` permission.
The pipeline runs as this identity.

### Jobs

The following permissions can be applied:
- `CAN VIEW` - see the job and run history
- `CAN MANAGE RUN` - trigger runs and cancel them
- `IS OWNER` - the job runs under this identity; one owner only
- `CAN MANAGE` - full control including permissions

The creator of a job automatically gets a `IS OWNER` permission.
The job runs as this identity.

---

## Fine-Grained Access Control
 
[Row Filters and Column Masks](https://docs.databricks.com/aws/en/data-governance/unity-catalog/filters-and-masks)\
[ABAC Policies](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/core-concepts)
 
Access control within a table -
restrict access to certain **columns** (masking) or **rows** (filtering)
based on the querying user's identity group membership.

### Dynamic Views

#### Masking a Column
 
There are 2 ways to mask a column.
 
1. Standard
```
CREATE VIEW <view> AS
SELECT
  ...,
  CASE WHEN
    is_account_group_member('admins') THEN email
    ELSE 'REDACTED'
  END AS email
FROM <table>;
 
CREATE VIEW <view> AS
SELECT
  ...
  CASE
    WHEN is_account_group_member('admins') THEN email
    ELSE regexp_extract(email, '^.*@(.*)$', 1)
  END
FROM <table>;
```
 
2. Applying a function
```
CREATE OR REPLACE FUNCTION mask_email(email STRING) RETURNS STRING
RETURN
CASE WHEN is_account_group_member('admins') THEN email
ELSE '***@***.***
END;
 
ALTER TABLE <table> ALTER COLUMN email SET MASK mask_email;
ALTER TABLE <table> ALTER COLUMN email DROP MASK;
```
 
#### Filtering Rows
 
There are 2 ways to filter rows.
 
1. Standard
```
CREATE VIEW <view> AS
SELECT
  ...
FROM <table-name>
WHERE
  CASE
    WHEN is_account_group_member('admins') THEN TRUE
    ELSE total <= 1000000
  END;
```
 
2. Applying a function
```
CREATE OR REPLACE FUNCTION geo_filter(country STRING) RETURNS BOOL
RETURN IF(is_account_group_member('admins'), true, country='France');
 
ALTER TABLE <table> SET ROW FILTER geo_filter ON(country);
ALTER TABLE <table> DROP ROW FILTER;
```
 
### ABAC Policies
 
ABAC (Attribute-Based Access Control) policies allow you
to define Column masks and Row filters based on table tags
(more specifically, so-called governed tags).

---

## System Tables

A Databricks-hosted analytical store of the account's operational data,
exposed as read-only Delta tables in the `system` catalog and governed by Unity Catalog.

Require a Unity Catalog-enabled workspace.

Account admins can read them by default.

Key Schemas:
- `system.billing` - cost & usage
  - `usage` - one row per usage record
    - `billing_origin_product` (_e.g._ `LAKEFLOW_CONNECT`)
    - `usage_type` (_e.g._ `COMPUTE_TIME`)
    - `usage_unit` (`DBU`, `MILLISECOND`)
    - `usage_quantity`
    - `usage_metadata`
  - `list_prices` - price history (join to `usage` for estimating costs)
- `system.lakeflow` - job & pipeline monitoring
  - `jobs` - job definitions
  - `job_tasks` - task definitions
  - `job_run_timeline` - job run history
  - `job_task_run_timeline` - task run history
  - `pipelines` - pipeline definitions
  - `pipeline_update_timeline` - pipeline updates
- `system.access` - governance
  - `audit` - who did what (audit logs)
  - `table_lineage` - table lineage
  - `column_lineage` - data lineage
- `system.compute` - compute config & events
  - `clusters`
  - `node_types`
  - `warehouse_events`
- `system.query`
  - `history` - SQL query history / performance

---