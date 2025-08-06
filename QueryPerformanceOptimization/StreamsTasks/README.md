# Snowflake Streams and Tasks Implementation

## Problem Statement

A CBA process is taking up to 20 minutes to run end-to-end primarily due to 10 minutes on orchestration overhead using Airflow. The process needs to finish much faster since the objective is to run it every 30 minutes, thereby leading to a target execution time of less than 5 minutes (end-to-end).

## Overview

This document describes the benefits of using streams and tasks to orchestrate data movements with near real-time capability. This approach would lead to reducing the 10 minute orchestration overhead using Airflow down to several seconds.

This repository contains a complete working example of Snowflake Streams and Tasks implementation that demonstrates how to:

1. **Read data** from Table A in RAW_DB using Snowflake Streams
2. **Process and load** data using INSERT TRUNCATE into Table B in STAGING_DB
3. **Automate the pipeline** using Snowflake Tasks
4. **Monitor and maintain** the data pipeline

## Assumptions

- **SNS integration** is needed to notify on errors
- **Streams and tasks** can be used to copy data incrementally or in bulk
- **Data in table B** needs to be overwritten with all new records coming into table A

## Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   RAW_DB        │    │   STREAM     │    │     TASK        │    │   STAGING_DB    │
│   TABLE_A       │───▶│ STREAM_      │───▶│ TASK_PROCESS_   │───▶│   TABLE_B       │
│                 │    │ TABLE_A      │    │ TABLE_A         │    │                 │
└─────────────────┘    └──────────────┘    └─────────────────┘    └─────────────────┘
```

## File Structure

| File | Purpose |
|------|---------|
| `01_setup_infrastructure.sql` | Creates databases, schemas, warehouses, and roles |
| `02_create_sample_tables.sql` | Creates sample Table A and Table B with test data |
| `03_create_stream.sql` | Creates and tests the Snowflake Stream on Table A |
| `04_create_sns_notification.sql` | Creates Amazon SNS integration for task error notifications |
| `05_create_task.sql` | Creates tasks and stored procedures for data processing |
| `06_test_and_monitor.sql` | Comprehensive testing and monitoring scripts |
| `07_cleanup.sql` | Cleanup script to remove all demo objects |

## Quick Start Guide

### Step 1: Infrastructure Setup
```sql
-- Run the infrastructure setup
@01_setup_infrastructure.sql
```

### Step 2: Create Sample Tables
```sql
-- Create and populate sample tables
@02_create_sample_tables.sql
```

### Step 3: Create Stream
```sql
-- Create stream to capture changes
@03_create_stream.sql
```

### Step 4: Create SNS Integration
```sql
-- Create Amazon SNS notification integration
@04_create_sns_notification.sql
```

### Step 5: Create Tasks
```sql
-- Create tasks and stored procedures with error notifications
@05_create_task.sql
```

### Step 6: Test and Monitor
```sql
-- Test the complete pipeline
@06_test_and_monitor.sql
```

## Implementation Steps

### Prerequisites
Before implementing this solution, ensure you have:
- **Snowflake Account** with appropriate privileges (ACCOUNTADMIN or equivalent)
- **AWS Account** for SNS integration setup
- **Database Access** to create objects in target Snowflake account
- **Understanding** of Snowflake Streams and Tasks concepts

### Detailed Implementation Process

#### Phase 1: Environment Preparation

**1.1 Snowflake Account Setup**
- Verify account has necessary compute resources
- Ensure appropriate role privileges are available
- Confirm warehouse sizing requirements based on data volume

**1.2 AWS Prerequisites (for SNS Integration)**
- Create AWS SNS topic for error notifications
- Set up IAM role with appropriate permissions
- Configure SNS subscriptions (email, SMS, etc.)

#### Phase 2: Infrastructure Deployment

**2.1 Execute Infrastructure Setup**
```sql
-- Connect as ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- Run infrastructure creation
@01_setup_infrastructure.sql
```

**Expected Outcomes:**
- RAW_DB and STAGING_DB databases created
- STREAMS_TASKS_WH warehouse provisioned
- STREAMS_TASKS_ROLE with appropriate privileges
- All necessary schemas established

**2.2 Validate Infrastructure**
```sql
-- Verify databases
SHOW DATABASES LIKE 'RAW_DB';
SHOW DATABASES LIKE 'STAGING_DB';

-- Verify warehouse
SHOW WAREHOUSES LIKE 'STREAMS_TASKS_WH';

-- Verify role
SHOW ROLES LIKE 'STREAMS_TASKS_ROLE';
```

#### Phase 3: Data Layer Setup

**3.1 Create Sample Tables**
```sql
-- Switch to appropriate role
USE ROLE STREAMS_TASKS_ROLE;
USE WAREHOUSE STREAMS_TASKS_WH;

-- Execute table creation
@02_create_sample_tables.sql
```

**Expected Outcomes:**
- TABLE_A created in RAW_DB.SAMPLE_DATA with sample data
- TABLE_B created in STAGING_DB.PROCESSED_DATA (initially empty)
- Sample transactional data loaded for testing

**3.2 Validate Table Structure**
```sql
-- Check Table A structure and data
DESCRIBE TABLE RAW_DB.SAMPLE_DATA.TABLE_A;
SELECT COUNT(*) FROM RAW_DB.SAMPLE_DATA.TABLE_A;

-- Check Table B structure
DESCRIBE TABLE STAGING_DB.PROCESSED_DATA.TABLE_B;
SELECT COUNT(*) FROM STAGING_DB.PROCESSED_DATA.TABLE_B;
```

#### Phase 4: Stream Configuration

**4.1 Create and Configure Stream**
```sql
-- Execute stream creation
@03_create_stream.sql
```

**Expected Outcomes:**
- STREAM_TABLE_A created on RAW_DB.SAMPLE_DATA.TABLE_A
- Stream captures INSERT, UPDATE, DELETE operations
- Initial stream state established

**4.2 Validate Stream Functionality**
```sql
-- Check stream exists
SHOW STREAMS IN SCHEMA RAW_DB.SAMPLE_DATA;

-- Verify stream captures changes
INSERT INTO RAW_DB.SAMPLE_DATA.TABLE_A (CUSTOMER_ID, AMOUNT, TRANSACTION_TYPE)
VALUES ('TEST_CUST', 100.00, 'TEST');

-- Check stream content
SELECT COUNT(*) FROM RAW_DB.SAMPLE_DATA.STREAM_TABLE_A;
```

#### Phase 5: Notification Integration

**5.1 AWS SNS Setup**
Before running the SQL script, complete these AWS steps:

1. **Create SNS Topic:**
   ```bash
   aws sns create-topic --name snowflake-task-errors
   ```

2. **Create IAM Role:** Use the trust policy provided in the script comments

3. **Note ARN Values:** Update the script with actual Topic ARN and Role ARN

**5.2 Execute SNS Integration**
```sql
-- Update ARNs in the script first, then execute
@04_create_sns_notification.sql
```

**Expected Outcomes:**
- STREAMS_TASKS_SNS_INTEGRATION created and enabled
- Integration linked to AWS SNS topic
- Proper IAM trust relationship established

**5.3 Test SNS Integration**
```sql
-- Test notification (uncomment in script)
CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
    'STREAMS_TASKS_SNS_INTEGRATION',
    'Test notification',
    'This is a test message'
);
```

#### Phase 6: Task Orchestration

**6.1 Create Task and Stored Procedure**
```sql
-- Execute task creation
@05_create_task.sql
```

**Expected Outcomes:**
- SP_PROCESS_TABLE_A_INCREMENTAL stored procedure created
- TASK_PROCESS_TABLE_A_INCREMENTAL task created (suspended state)
- Task configured with SNS error integration
- 2-minute execution schedule established

**6.2 Validate Task Configuration**
```sql
-- Check task exists
SHOW TASKS IN SCHEMA STAGING_DB.PROCESSED_DATA;

-- Verify task details
SELECT NAME, STATE, SCHEDULE, WAREHOUSE, CONDITION
FROM STAGING_DB.INFORMATION_SCHEMA.TASKS 
WHERE NAME = 'TASK_PROCESS_TABLE_A_INCREMENTAL';
```

#### Phase 7: Testing and Activation

**7.1 Manual Testing**
```sql
-- Execute comprehensive testing
@06_test_and_monitor.sql
```

**Testing Scenarios:**
- Manual task execution
- Stream data validation
- Error handling verification
- Performance monitoring

**7.2 Task Activation**
```sql
-- Enable the task for automatic execution
ALTER TASK STAGING_DB.PROCESSED_DATA.TASK_PROCESS_TABLE_A_INCREMENTAL RESUME;

-- Verify task is running
SELECT NAME, STATE, NEXT_SCHEDULED_TIME
FROM STAGING_DB.INFORMATION_SCHEMA.TASKS 
WHERE NAME = 'TASK_PROCESS_TABLE_A_INCREMENTAL';
```

#### Phase 8: Production Monitoring

**8.1 Set Up Monitoring Queries**
```sql
-- Create monitoring view for ongoing operations
SELECT * FROM STAGING_DB.PROCESSED_DATA.V_PIPELINE_MONITORING;

-- Check task execution history
SELECT * FROM TABLE(STAGING_DB.INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_PROCESS_TABLE_A_INCREMENTAL',
    SCHEDULED_TIME_RANGE_START => DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
));
```

**8.2 Operational Procedures**
- **Daily Monitoring:** Check task execution status and data freshness
- **Error Response:** Monitor SNS notifications and respond to failures
- **Performance Tuning:** Adjust warehouse size and task frequency as needed
- **Data Quality:** Validate record counts and data integrity regularly

#### Phase 9: Maintenance and Cleanup

**9.1 Regular Maintenance Tasks**
- Monitor stream staleness and refresh if needed
- Review task execution patterns and optimize schedules
- Clean up old batch data if retention policies require it
- Update SNS integration credentials as needed

**9.2 Environment Cleanup (if needed)**
```sql
-- Remove all demo objects
@07_cleanup.sql
```

### Implementation Timeline

| Phase | Estimated Duration | Dependencies |
|-------|-------------------|--------------|
| Prerequisites | 30 minutes | AWS access, Snowflake admin rights |
| Infrastructure | 15 minutes | Account privileges |
| Data Layer | 10 minutes | Infrastructure complete |
| Stream Setup | 10 minutes | Tables created |
| SNS Integration | 30 minutes | AWS configuration |
| Task Creation | 15 minutes | All previous phases |
| Testing | 30 minutes | Full pipeline deployed |
| Production | Ongoing | Monitoring procedures |

**Total Implementation Time: ~2.5 hours** (including AWS setup and testing)

## Key Components

### 1. Infrastructure Objects

- **RAW_DB**: Source database containing Table A
- **STAGING_DB**: Target database containing Table B
- **STREAMS_TASKS_WH**: Dedicated warehouse for task execution
- **STREAMS_TASKS_ROLE**: Custom role with appropriate privileges

### 2. Data Tables

#### Table A (Source)
```sql
CREATE TABLE RAW_DB.SAMPLE_DATA.TABLE_A (
    ID INTEGER AUTOINCREMENT,
    CUSTOMER_ID VARCHAR(50),
    TRANSACTION_DATE TIMESTAMP_NTZ,
    AMOUNT DECIMAL(10,2),
    TRANSACTION_TYPE VARCHAR(20),
    STATUS VARCHAR(10) DEFAULT 'ACTIVE',
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

#### Table B (Target)
```sql
CREATE TABLE STAGING_DB.PROCESSED_DATA.TABLE_B (
    -- All columns from Table A plus:
    PROCESSED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    BATCH_ID VARCHAR(50),
    SOURCE_SYSTEM VARCHAR(20) DEFAULT 'RAW_DB',
    STREAM_ACTION VARCHAR(10),
    STREAM_TIMESTAMP TIMESTAMP_NTZ
);
```

### 3. Stream Configuration

```sql
CREATE STREAM STREAM_TABLE_A 
ON TABLE RAW_DB.SAMPLE_DATA.TABLE_A
COMMENT = 'Stream to capture all changes (INSERT, UPDATE, DELETE) on Table A';
```

### 4. Task Configuration

#### Incremental Processing Task
```sql
CREATE TASK TASK_PROCESS_TABLE_A_INCREMENTAL
    WAREHOUSE = STREAMS_TASKS_WH
    SCHEDULE = '2 MINUTE'
    ERROR_INTEGRATION = STREAMS_TASKS_SNS_INTEGRATION
    WHEN SYSTEM$STREAM_HAS_DATA('RAW_DB.SAMPLE_DATA.STREAM_TABLE_A')
    AS
    CALL STAGING_DB.PROCESSED_DATA.SP_PROCESS_TABLE_A_INCREMENTAL();
```

### 5. SNS Error Notification Integration

The implementation includes Amazon SNS integration for automatic error notifications:

```sql
CREATE NOTIFICATION INTEGRATION STREAMS_TASKS_SNS_INTEGRATION
    TYPE = QUEUE
    NOTIFICATION_PROVIDER = AWS_SNS
    DIRECTION = OUTBOUND
    AWS_SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:123456789012:snowflake-task-errors'
    AWS_SNS_ROLE_ARN = 'arn:aws:iam::123456789012:role/SnowflakeNotificationRole'
    ENABLED = TRUE;
```

**Error Notification Features:**
- Automatic alerts when tasks fail
- Detailed error messages and stack traces
- Task execution context and timing information
- Integration with AWS SNS for flexible notification delivery (email, SMS, etc.)

**AWS Setup Requirements:**
1. Create SNS topic in AWS Console
2. Create IAM role with appropriate trust policy
3. Configure SNS publish permissions
4. Subscribe to SNS topic for notifications
5. Update integration with actual ARN values

## Processing Logic

### Incremental Processing Approach
The stored procedure `SP_PROCESS_TABLE_A_INCREMENTAL()` implements:

1. **Stream-Based Processing**: Processes only changed records from the stream
2. **Change Tracking**: Preserves METADATA$ACTION (INSERT/UPDATE/DELETE)
3. **Incremental Batching**: Smaller, more frequent updates every 2 minutes
4. **Batch Tracking**: Assigns unique batch ID for each run
5. **Metadata**: Adds processing timestamps and source information

## Monitoring and Maintenance

### Key Monitoring Queries

```sql
-- Check pipeline status
SELECT * FROM STAGING_DB.PROCESSED_DATA.V_PIPELINE_MONITORING;

-- Task execution history
SELECT * FROM TABLE(STAGING_DB.INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_PROCESS_TABLE_A_INCREMENTAL',
    SCHEDULED_TIME_RANGE_START => DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
));

-- Stream health check
SELECT STREAM_NAME, STALE, STALE_AFTER 
FROM RAW_DB.INFORMATION_SCHEMA.STREAMS 
WHERE STREAM_NAME = 'STREAM_TABLE_A';
```

### Data Quality Checks

- **Record Count Validation**: Compares source vs target counts
- **Data Freshness**: Monitors last processing time
- **Stream Staleness**: Alerts on stale streams

## Task Management

### Enable/Disable Task
```sql
-- Enable task
ALTER TASK STAGING_DB.PROCESSED_DATA.TASK_PROCESS_TABLE_A_INCREMENTAL RESUME;

-- Disable task
ALTER TASK STAGING_DB.PROCESSED_DATA.TASK_PROCESS_TABLE_A_INCREMENTAL SUSPEND;
```

### Manual Execution
```sql
-- Execute task manually
EXECUTE TASK STAGING_DB.PROCESSED_DATA.TASK_PROCESS_TABLE_A_INCREMENTAL;
```

## Testing Scenarios

The implementation includes comprehensive testing for:

1. **INSERT Operations**: New records added to Table A
2. **UPDATE Operations**: Existing records modified
3. **DELETE Operations**: Records marked as inactive (soft delete)
4. **Batch Processing**: Multiple changes processed together
5. **Error Handling**: Exception management and logging

## Performance Considerations

### Warehouse Sizing
- **XSMALL**: Suitable for small datasets (< 10,000 records)
- **SMALL**: Recommended for medium datasets (10,000 - 100,000 records)
- **MEDIUM+**: For larger datasets or complex transformations

### Task Scheduling
- **High Frequency**: 1-2 minutes for near real-time processing
- **Standard**: 5-15 minutes for regular batch processing
- **Low Frequency**: 30+ minutes for large datasets

### Stream Management
- Monitor stream staleness to prevent data loss
- Consider stream retention policies for long-term storage
- Implement error handling for stream consumption failures

## Security Best Practices

1. **Role-Based Access**: Use dedicated roles with minimal privileges
2. **Warehouse Isolation**: Separate warehouses for different workloads
3. **Schema Separation**: Isolate raw and processed data
4. **Audit Logging**: Track all data processing activities

## Troubleshooting

### Common Issues

1. **Task Not Running**: Check if task is resumed and has proper privileges
2. **Stream Stale**: Recreate stream if it becomes stale
3. **Warehouse Suspended**: Ensure warehouse auto-resume is enabled
4. **Permission Errors**: Verify role has all required privileges

### Debug Queries

```sql
-- Check task status
SHOW TASKS IN SCHEMA STAGING_DB.PROCESSED_DATA;

-- View task errors
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) 
WHERE STATE = 'FAILED';

-- Stream diagnostics
SELECT * FROM RAW_DB.INFORMATION_SCHEMA.STREAMS;
```

## Cleanup

To remove all demo objects:

```sql
@07_cleanup.sql
```

**Warning**: This will permanently delete all created databases, tables, streams, tasks, and related objects.

## Best Practices

1. **Test in Development**: Always test streams and tasks in a development environment first
2. **Monitor Resource Usage**: Track warehouse credits and optimize task frequency
3. **Implement Alerting**: Set up notifications for task failures or data quality issues
4. **Version Control**: Maintain all SQL scripts in version control
5. **Documentation**: Keep detailed documentation of business logic and dependencies

## Support and Maintenance

- Review task execution history regularly
- Monitor stream staleness and data freshness
- Optimize task schedules based on data volume and business requirements
- Implement proper error handling and retry logic
- Regular cleanup of old batch data if needed

---

## Author
Generated Sample Code for Snowflake Streams and Tasks Implementation

## Version
1.0 - Initial Implementation

## Last Updated
2024 