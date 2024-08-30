---
title: "Steampipe Table: aws_appflow_flow - Query AWS AppFlow flows using SQL"
description: "Allows users to query AWS AppFlow flows for detailed information about each flow, including its status, most recent execution status, source and destination connectors"
---

# Table: aws_appflow_flow - Query AWS AppFlow flows using SQL

The AWS AppFlow flow is a part of Amazon AppFlow, a fully managed integration service to transfer data between services such as Salesforce, SAP, Google Analytics, and Amazon Redshift. It defines the source and the destination of data, as well as how the data transfer is triggered.


## Table Usage Guide

The `aws_appflow_flow` table in Steampipe provides you with information about flows within AWS AppFlow. This table allows you, as a DevOps engineer, to query flow-specific details, including the flow's status, description, source and destination connectors, most recent execution status, and more. The schema outlines the various attributes of the AppFlow Flow for you, including the flow ARN, creation time and associated tags.

## Examples

### Basic info
Explore the characteristics of your AWS AppFlow flows, such as its creation time, status, connectors and trigger types. This can help you understand the status of your flows' executions

```sql+postgres
select
  name,
  arn,
  destination_connector_type,
  source_connector_type,
  status,
  most_recent_execution_status,
  trigger_type
from
  aws_appflow_flow;
```

```sql+sqlite
select
  name,
  arn,
  destination_connector_type,
  source_connector_type,
  status,
  most_recent_execution_status,
  trigger_type
from
  aws_appflow_flow;
```

### List flows whose most recent execution failed with SAP OData source connector
Allows quickly visualising failures associated with a particular source connector

```sql+postgres
select
  name,
  arn,
  source_connector_type,
  most_recent_execution_status,
  trigger_type
from
  aws_appflow_flow
where source_connector_type = 'SAPOData'
and most_recent_execution_status = 'Error'
```

```sql+sqlite
select
  name,
  arn,
  source_connector_type,
  most_recent_execution_status,
  trigger_type
from
  aws_appflow_flow
where source_connector_type = 'SAPOData'
and most_recent_execution_status = 'Error'
```