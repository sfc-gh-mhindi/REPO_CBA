-- ============================================================================
-- Snowflake Streams and Tasks Implementation - Testing and Monitoring
-- ============================================================================
-- Purpose: Test the complete pipeline and monitor its execution
-- Author: Generated Sample Code
-- Created: 2024
-- ============================================================================

-- Set context
USE ROLE STREAMS_TASKS_ROLE;
USE WAREHOUSE STREAMS_TASKS_WH;

-- ============================================================================
-- 1. ENABLE TASKS FOR TESTING
-- ============================================================================

-- Enable the task (uncomment to activate)
-- ALTER TASK STAGING_DB.PROCESSED_DATA.TASK_PROCESS_TABLE_A_INCREMENTAL RESUME;

-- Check task status
SELECT 'Task Status Check:' AS info;
SELECT 
    NAME,
    STATE,
    SCHEDULE,
    CONDITION,
    LAST_COMMITTED_ON,
    NEXT_SCHEDULED_TIME
FROM STAGING_DB.INFORMATION_SCHEMA.TASKS 
WHERE NAME = 'TASK_PROCESS_TABLE_A_INCREMENTAL';

-- ============================================================================
-- 2. MANUAL TESTING - INSERT NEW DATA
-- ============================================================================

-- Insert new test data to trigger stream changes
INSERT INTO RAW_DB.SAMPLE_DATA.TABLE_A (CUSTOMER_ID, TRANSACTION_DATE, AMOUNT, TRANSACTION_TYPE)
VALUES 
    ('CUST007', CURRENT_TIMESTAMP(), 450.00, 'PURCHASE'),
    ('CUST008', CURRENT_TIMESTAMP(), 750.25, 'DEPOSIT'),
    ('CUST009', CURRENT_TIMESTAMP(), 125.75, 'WITHDRAWAL');

-- Check stream content after new inserts
SELECT 'Stream content after new inserts:' AS info;
SELECT 
    METADATA$ACTION,
    METADATA$ISUPDATE,
    ID,
    CUSTOMER_ID,
    AMOUNT,
    TRANSACTION_TYPE
FROM RAW_DB.SAMPLE_DATA.STREAM_TABLE_A
ORDER BY ID;

-- ============================================================================
-- 3. MANUAL TASK EXECUTION FOR TESTING
-- ============================================================================

-- Execute task manually for immediate testing
EXECUTE TASK STAGING_DB.PROCESSED_DATA.TASK_PROCESS_TABLE_A_INCREMENTAL;

-- Check if data was processed into Table B
SELECT 'Data in Table B after task execution:' AS info;
SELECT 
    ID,
    CUSTOMER_ID,
    AMOUNT,
    TRANSACTION_TYPE,
    BATCH_ID,
    STREAM_ACTION,
    PROCESSED_AT
FROM STAGING_DB.PROCESSED_DATA.TABLE_B
ORDER BY PROCESSED_AT DESC, ID
LIMIT 10;

-- ============================================================================
-- 4. STREAM STATUS MONITORING
-- ============================================================================

-- Check stream status and content
SELECT 'Stream monitoring:' AS category;
SELECT 
    'Records in stream' AS metric,
    COUNT(*) AS value
FROM RAW_DB.SAMPLE_DATA.STREAM_TABLE_A
UNION ALL
SELECT 
    'Records in Table A' AS metric,
    COUNT(*) AS value
FROM RAW_DB.SAMPLE_DATA.TABLE_A
UNION ALL
SELECT 
    'Records in Table B' AS metric,
    COUNT(*) AS value
FROM STAGING_DB.PROCESSED_DATA.TABLE_B;

-- Stream metadata information
SELECT 
    'Stream Metadata' AS category,
    STREAM_NAME,
    TABLE_NAME,
    CREATED_ON,
    STALE,
    MODE
FROM RAW_DB.INFORMATION_SCHEMA.STREAMS 
WHERE STREAM_NAME = 'STREAM_TABLE_A';

-- ============================================================================
-- 5. TASK EXECUTION HISTORY AND MONITORING
-- ============================================================================

-- Check task execution history
SELECT 'Recent task executions:' AS info;
SELECT 
    NAME,
    STATE,
    SCHEDULED_TIME,
    QUERY_START_TIME,
    COMPLETED_TIME,
    RUN_ID,
    RETURN_VALUE
FROM TABLE(STAGING_DB.INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'TASK_PROCESS_TABLE_A_INCREMENTAL',
    SCHEDULED_TIME_RANGE_START => DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC
LIMIT 10;

-- ============================================================================
-- 6. DATA QUALITY CHECKS
-- ============================================================================

-- Compare record counts between source and target
SELECT 'Data Quality Checks:' AS category;
WITH source_data AS (
    SELECT COUNT(*) AS source_count
    FROM RAW_DB.SAMPLE_DATA.TABLE_A
    WHERE STATUS = 'ACTIVE'
),
target_data AS (
    SELECT COUNT(*) AS target_count
    FROM STAGING_DB.PROCESSED_DATA.TABLE_B
)
SELECT 
    s.source_count,
    t.target_count,
    CASE 
        WHEN s.source_count = t.target_count THEN '✓ PASS'
        ELSE '✗ FAIL'
    END AS data_integrity_check
FROM source_data s, target_data t;

-- Check for data freshness
SELECT 'Data Freshness Check:' AS category;
SELECT 
    MAX(PROCESSED_AT) AS last_processed_time,
    DATEDIFF('MINUTE', MAX(PROCESSED_AT), CURRENT_TIMESTAMP()) AS minutes_since_last_update,
    CASE 
        WHEN DATEDIFF('MINUTE', MAX(PROCESSED_AT), CURRENT_TIMESTAMP()) <= 10 THEN '✓ FRESH'
        ELSE '⚠ STALE'
    END AS freshness_status
FROM STAGING_DB.PROCESSED_DATA.TABLE_B;

-- ============================================================================
-- 7. PERFORMANCE MONITORING
-- ============================================================================

-- Warehouse usage for tasks
SELECT 'Warehouse Usage:' AS info;
SELECT 
    WAREHOUSE_NAME,
    SUM(CREDITS_USED) AS total_credits_used,
    COUNT(*) AS query_count,
    AVG(EXECUTION_TIME) AS avg_execution_time_ms
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE WAREHOUSE_NAME = 'STREAMS_TASKS_WH'
    AND START_TIME >= DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
GROUP BY WAREHOUSE_NAME;

-- ============================================================================
-- 8. ADDITIONAL TEST SCENARIOS
-- ============================================================================

-- Test UPDATE scenario
UPDATE RAW_DB.SAMPLE_DATA.TABLE_A 
SET AMOUNT = AMOUNT * 1.1, UPDATED_AT = CURRENT_TIMESTAMP()
WHERE CUSTOMER_ID = 'CUST007';

-- Test DELETE scenario (soft delete by changing status)
UPDATE RAW_DB.SAMPLE_DATA.TABLE_A 
SET STATUS = 'INACTIVE', UPDATED_AT = CURRENT_TIMESTAMP()
WHERE CUSTOMER_ID = 'CUST008';

-- Check stream after updates
SELECT 'Stream content after UPDATE and DELETE operations:' AS info;
SELECT 
    METADATA$ACTION,
    METADATA$ISUPDATE,
    ID,
    CUSTOMER_ID,
    AMOUNT,
    STATUS,
    TRANSACTION_TYPE
FROM RAW_DB.SAMPLE_DATA.STREAM_TABLE_A
ORDER BY METADATA$ACTION, ID;

-- ============================================================================
-- 9. CLEANUP AND MAINTENANCE QUERIES
-- ============================================================================

-- Query to check stream staleness
SELECT 'Stream Health Check:' AS category;
SELECT 
    STREAM_NAME,
    STALE,
    STALE_AFTER,
    CASE 
        WHEN STALE = 'false' THEN '✓ HEALTHY'
        ELSE '⚠ STALE'
    END AS stream_health
FROM RAW_DB.INFORMATION_SCHEMA.STREAMS 
WHERE STREAM_NAME = 'STREAM_TABLE_A';

-- ============================================================================
-- 10. USEFUL MONITORING QUERIES FOR ONGOING OPERATIONS
-- ============================================================================

-- Create a view for ongoing monitoring
CREATE OR REPLACE VIEW STAGING_DB.PROCESSED_DATA.V_PIPELINE_MONITORING AS
SELECT 
    'Source Records (Active)' AS metric,
    COUNT(*) AS value,
    CURRENT_TIMESTAMP() AS check_time
FROM RAW_DB.SAMPLE_DATA.TABLE_A
WHERE STATUS = 'ACTIVE'
UNION ALL
SELECT 
    'Target Records' AS metric,
    COUNT(*) AS value,
    CURRENT_TIMESTAMP() AS check_time
FROM STAGING_DB.PROCESSED_DATA.TABLE_B
UNION ALL
SELECT 
    'Stream Changes Pending' AS metric,
    COUNT(*) AS value,
    CURRENT_TIMESTAMP() AS check_time
FROM RAW_DB.SAMPLE_DATA.STREAM_TABLE_A;

-- Query the monitoring view
SELECT 'Pipeline Monitoring Dashboard:' AS info;
SELECT * FROM STAGING_DB.PROCESSED_DATA.V_PIPELINE_MONITORING;

SELECT 'Testing and monitoring setup completed!' AS status; 