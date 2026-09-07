# Lakeflow Jobs

Lakeflow Jobs is workflow automation for Databricks,
providing orchestration for data processing workloads to run multiple tasks.

## Table of Contents

> - [Concepts](#concepts)
> - [Control Flow](#control-flow)
>   - [Run if Conditional Tasks](#run-if-conditional-tasks)
>   - [If/Else](#ifelse)
>   - [For Each](#for-each)
> - [Commands](#commands)
> - [Parameters](#parameters)
>   - [Job Parameters](#job-parameters)
>     - [Setting Job Parameters](#setting-job-parameters)
>     - [Overriding Job Parameters](#overriding-job-parameters)
>     - [Reading Job Parameters](#reading-job-parameters)
>     - [Existing Job Parameters](#existing-job-parameters)
>   - [Task Parameters](#task-parameters)
>     - [Setting Task Parameters](#setting-task-parameters)
>     - [Reading Task Parameters](#reading-task-parameters)
>     - [Existing Task Parameters](#existing-task-parameters)
> - [Bundles](#bundles)
>   - [Bundle Configurations](#bundle-configurations) 
>   - [Templates](#templates)
>     - [Template File](#template-file)
>     - [databricks.yml.tmpl](#templateproject_namedatabricksymltmpl)
>     - [job.yml.tmpl](#templateproject_nameresourcesproject_name_jobymltmpl)

---

## Concepts:

[Schedules and Triggers](https://docs.databricks.com/aws/en/jobs/triggers)
[Fan-In and Fan-Out Architectures](https://docs.databricks.com/aws/en/data-engineering/fan-in-fan-out)
[Repair Job Failures](https://docs.databricks.com/aws/en/jobs/repair-job-failures)

- **Job**:
  the primary resource for coordinating, scheduling, and running your operations
- **Task**:
  a specific unit of work within a job
  - notebook
  - pipeline (can't run in parallel when a job is set to have concurrent runs)
  - Python script
  - ...
- **Trigger**:
  defines how a job should be launched
  - manual (one time run)
  - scheduled/periodic (time-based, cron, _etc._)
  - continuous (keeps a run always active)
  - event-based
    - table update
    - file arrival to a location, monitored by the Unity Catalog
    - model update
  - run as a task (triggered by another job)

To run a job:
```
databricks bundle run <job-name>
```

To run partial job:
```
databricks bundle run <job-name> --only [+]<task-name>[+],...
```

Jobs can reference code, defined in remote Git repositories.\
Databricks recommends using remote Git repositories over Git folders.

Jobs can contain max 1000 tasks.

Jobs can run concurrently in parallel (by default it's set to 1).

Jobs and tasks have:
- **Run time**:
  the expected run time
- **Timeout**: 
  the maximum run time

Job phases:
- Queued (waiting for the start because of concurrency limits)
- Waiting for resources (waiting for the compute resource to become available)
- Library installation
- Running

---

## Control Flow

Tasks form a Directed Acyclic Graph (DAG) - 
each task can declare `depends_on` other tasks and runs only after they complete.

The graph can have the following architectures:
- **Simple**: one task -> one task
- **Fan-out**: one task -> many downstream tasks (parallel branches)
- **Fan-in**: many tasks → one downstream task (it waits for all of them)

### Run if conditional tasks

Tasks, declared in `depends_on` should have the following state
before the dependent task can start:
- all succeeded
- at least one succeeded
- none failed
- all done
- at least one failed
- all failed

### If/Else

A task that launches a dependant task only if a condition
checking a value stored as a job/task parameter is satisfied.

Available operators:
- `==`
- `!=`
- `>`
- `>=`
- `<`
- `<=`

They could reference:
- job parameters (`{{job.parameters.<value-name>}}`)
- task parameters (`{{tasks.<task-name>.values.<value-name>}}`)

### For each

[Reference](https://docs.databricks.com/aws/en/jobs/tasks/for-each)

To reference a parameter, passed to a nested task use `{{input.<key-name>}}`.

Downstream tasks can not reference the nested tasks.

---

## Commands

Create a new job from template:
```
databricks bundle init [<bundle-template>]
```

Create a new job, containing a pipeline:
```
databricks pipelines init
```

Validate an existing job if it follows formatting rules:
```
databricks bundle validate
```

Deploy a job to a target environment:
```
databricks bundle deploy --target <env> [--auto-approve] [--fail-on-active-runs]
```

Run a job on a target environment:
```
databricks bundle run --target <env> <job-name>
```

Destroy an existing job:
```
databricks bundle destroy --target <env>
```

Generate a bundle from an existing job and bind it:
```
databricks bundle generate job --existing-job-id <job-id>
databricks bundle deployment bind <internal-job-name> <external-job-id>
```

Generate a bundle from an existing pipeline:
```
databricks bundle generate pipeline --existing-pipeline-id <pipeline-id>
```

---

## Parameters

There are 2 levels:
- job parameters
- task parameters

a job parameters takes the precedence over a task parameter.

### Job Parameters

Key-value pairs defined at the job level, applied to every task in the job.

#### Setting Job Parameters

###### UI

Jobs & Pipelines -> <job> -> Job details -> Edit configurations

###### Bundle YAML

```
resources:
  jobs:
    <job-name>:
      parameters:
        - name: <key1>
          default: <value1>
        - name: <key2>
          default: <value2>
```

###### REST API

Setting the parameters when creating a job.

#### Overriding Job Parameters

###### UI

Run now -> Run with different parameters

###### CLI

`databricks bundle run <job-name> --params <key1>=<value1>,<key2>=<value2>,...`

###### REST API

`job_parameters` field of the `run-now` endpoint

#### Reading Job Parameters

###### dbutils

You can use access job parameters in Notebooks through widgets.

```
var_name = dbutils.widgets.get("<job-parameter-key>")
```

Setting a default value:
```
var_name = dbutils.widgets.text("<job-parameter-key>", "<default-value>")
```

###### SQL

In SQL job parameters are accessed through named parameters.

```
SELECT * FROM <table>
WHERE <column-name> >= :<job-parameter-name>;
```

###### Python scripts

```  
import argparse
arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--<job-parameter-name>")
args = arg_parser.parse_args()
```

###### Dynamic Value References

These are accessible in configuration YAML files through `{{}}`.

```
tasks:
  - task_key: <task-key>
    job_cluster_key: {{job.parameters.<job-parameter-name}}
    notebook_task:
      notebook_path: <notebook-path>
```

#### Existing Job Parameters

There are some parameters that exist at the job creation/run
that you can reference in your code:
- `job.id`
- `job.name`
- `job.start_time.<time>` (`iso_date`, `iso_datetime`, `year`, _etc._)
- ...

### Task Parameters

Key-value pairs that are set within tasks.

#### Setting Task Parameters

Within a task run this command:
```
dbutils.jobs.taskValues.set(key='<key>', value='<value>')
```

#### Reading Task Parameters

###### dbutils

Referencing a key, created by another task:
```
var_name = dbutils.jobs.taskValues.get(taskKey='<task-name>', key='<key>')
```

You can use widgets but job parameters takes precedence:
```
var_name = dbutils.widgets.get("<task-parameter-key>")
```

###### SQL 

In SQL task parameters are accessed through named parameters
(the same as for job parameters).

```
SELECT * FROM <table>
WHERE <column-name> >= :<task-parameter-name>;
```

###### Dynamic Value References

These are accessible in configuration YAML files through `{{}}`.

```
resources:
  jobs:
    <job-name>:
      tasks:
        - task_key: <task-name-1>
          notebook_task:
            notebook_path: <notebook-path-1>
        - task_key: <task-name-2>
          depends_on:
            - task_key: <task-name-1>
          notebook_task:
            notebook_path: <notebook-path-2>
            base_parameters:
              row_count: "{{tasks.<task-name-1>.values.<key>}}"
```

#### Existing Task Parameters

There are some parameters that exist at the task creation/run
that you can reference in your code:
- `tasks.<task-name>.result_state`
- ...

---

## Bundles

Declarative Automation Bundles (DABs) are a tool to facilitate 
the adoption of software engineering best practices, including:
- source control
- code review
- testing
- CI/CD

### Bundle Configurations

[Bundle Configurations](https://docs.databricks.com/aws/en/dev-tools/bundles/settings)

A bundle must contain one configuration file named `databricks.yml`
at the root of the bundle project folder:

Main structure (some fields are omitted):
```
bundle:
  name: string

run_as:
  - user_name: <user-name>
  - service_principal_name: <service-principal-name>

include:
  - <resources-path>.yml
  - ...

variables:
  <variable-name>:
    description: string
    default: string or complex
    lookup: Map
    type: string

permissions:
  - level: <permission-level>
    group_name: <group-name>
  - level: <permission-level>
    user_name: <user-name>
  - level: <permission-level>
    service_principal_name: <service-principal-name>

resources:
  jobs:
    <job-name>:
      # job settings

targets:
  <environment-name>:
    mode: string
    variables:
      <variable-name>: <variable-value>
    workspace:
      # workspace settings for this target
    run_as:
      user_name|service_principal_name: <user-name|service-principal-name>
```

### Templates

```
basic-bundle-template
  ├── databricks_template_schema.json
  └── template
      └── {{.project_name}}
          ├── databricks.yml.tmpl
          ├── resources
          │   └── {{.project_name}}_job.yml.tmpl
          └── src
              └── simple_notebook.ipynb
```

#### Template File
```
{
  "properties": {
    "project_name": {
      "type": "string",
      "default": "basic_bundle",
      "description": "What is the name of the bundle you want to create?",
      "order": 1
    }
  },
  "success_message": "\nYour bundle '{{.project_name}}' has been created."
}
```

#### template/{{.project_name}}/databricks.yml.tmpl
```
# databricks.yml
# This is the configuration for the bundle {{.project_name}}.

bundle:
  name: {{.project_name}}

include:
  - resources/*.yml

targets:
  # The deployment targets. See https://docs.databricks.com/en/dev-tools/bundles/deployment-modes.html
  dev:
    mode: development
    default: true
    workspace:
      host: {{workspace_host}}

  prod:
    mode: production
    workspace:
      host: {{workspace_host}}
      # Deploy to a folder whose write access is restricted to the deploying
      # identity. Avoid /Shared, which is writable by all workspace users.
      root_path: /Workspace/Production/.bundle/${bundle.name}
    {{- if not is_service_principal}}
    run_as:
      # This runs as {{user_name}} in production. Alternatively,
      # a service principal could be used here using service_principal_name
      user_name: {{user_name}}
    {{end -}}
```


### template/{{.project_name}}/resources/{{.project_name}}_job.yml.tmpl
```
# {{.project_name}}_job.yml
# The main job for {{.project_name}}

resources:
    jobs:
        {{.project_name}}_job:
        name: {{.project_name}}_job
        tasks:
            - task_key: notebook_task
            job_cluster_key: job_cluster
            notebook_task:
                notebook_path: ../src/simple_notebook.ipynb
        job_clusters:
            - job_cluster_key: job_cluster
            new_cluster:
                node_type_id: i3.xlarge
                spark_version: 13.3.x-scala2.12
```