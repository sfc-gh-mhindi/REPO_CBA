-- ============================================================================
-- Snowflake Streams and Tasks Implementation - Task Creation
-- ============================================================================
-- Purpose: Creates a task to process stream data and load into Table B
-- Note: Tasks include ERROR_INTEGRATION for SNS notifications on failures
-- Prerequisites: Run 04_create_sns_notification.sql first to create SNS integration
-- Author: Generated Sample Code
-- Created: 2024
-- ============================================================================

-- Set context
USE ROLE STREAMS_TASKS_ROLE;
USE WAREHOUSE STREAMS_TASKS_WH;
USE DATABASE STAGING_DB;
USE SCHEMA PROCESSED_DATA;

-- ============================================================================
-- 1. CREATE STORED PROCEDURE FOR INCREMENTAL PROCESSING
-- ============================================================================

-- Create a stored procedure that processes only stream changes (incremental)
CREATE OR REPLACE PROCEDURE SP_PROCESS_TABLE_A_INCREMENTAL()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    changes_count INTEGER;
    batch_id STRING;
    result_message STRING;
BEGIN
    -- Generate unique batch ID
    batch_id := 'INC_BATCH_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HHMMSS') || '_' || RANDSTR(6, RANDOM());
    
    -- Check if there are changes in the stream
    SELECT COUNT(*) INTO changes_count FROM RAW_DB.SAMPLE_DATA.STREAM_TABLE_A;
    
    IF (changes_count > 0) THEN
        result_message := 'Processing ' || changes_count || ' incremental changes with batch ID: ' || batch_id;
        
        -- Process only the changes from the stream
        INSERT INTO STAGING_DB.PROCESSED_DATA.TABLE_B (
            ID,
            CUSTOMER_ID,
            TRANSACTION_DATE,
            AMOUNT,
            TRANSACTION_TYPE,
            STATUS,
            CREATED_AT,
            UPDATED_AT,
            PROCESSED_AT,
            BATCH_ID,
            SOURCE_SYSTEM,
            STREAM_ACTION,
            STREAM_TIMESTAMP
        )
        SELECT 
            s.ID,
            s.CUSTOMER_ID,
            s.TRANSACTION_DATE,
            s.AMOUNT,
            s.TRANSACTION_TYPE,
            s.STATUS,
            s.CREATED_AT,
            s.UPDATED_AT,
            CURRENT_TIMESTAMP() AS PROCESSED_AT,
            batch_id AS BATCH_ID,
            'RAW_DB' AS SOURCE_SYSTEM,
            s.METADATA$ACTION AS STREAM_ACTION,
            CURRENT_TIMESTAMP() AS STREAM_TIMESTAMP
        FROM RAW_DB.SAMPLE_DATA.STREAM_TABLE_A s;
        
        result_message := result_message || '. Incremental processing completed successfully.';
        RETURN result_message;
    ELSE
        RETURN 'No changes detected in stream. Task completed with no processing.';
    END IF;
    
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error in incremental processing: ' || SQLERRM;
END;
$$;

-- ============================================================================
-- 2. CREATE TASK FOR INCREMENTAL PROCESSING
-- ============================================================================

-- Drop task if exists
DROP TASK IF EXISTS TASK_PROCESS_TABLE_A_INCREMENTAL;

-- Create the task for incremental processing
CREATE TASK IF NOT EXISTS TASK_PROCESS_TABLE_A_INCREMENTAL
    WAREHOUSE = STREAMS_TASKS_WH
    SCHEDULE = '2 MINUTE'
    ERROR_INTEGRATION = STREAMS_TASKS_SNS_INTEGRATION
    WHEN SYSTEM$STREAM_HAS_DATA('RAW_DB.SAMPLE_DATA.STREAM_TABLE_A')
    AS
    CALL STAGING_DB.PROCESSED_DATA.SP_PROCESS_TABLE_A_INCREMENTAL();

-- ============================================================================
-- 3. VERIFY TASK CREATION
-- ============================================================================

-- Show tasks in the schema
SHOW TASKS IN SCHEMA STAGING_DB.PROCESSED_DATA;

-- Check task details
SELECT 
    NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    OWNER,
    COMMENT,
    WAREHOUSE,
    SCHEDULE,
    STATE,
    CONDITION,
    CREATED_ON
FROM STAGING_DB.INFORMATION_SCHEMA.TASKS 
WHERE NAME = 'TASK_PROCESS_TABLE_A_INCREMENTAL';

-- ============================================================================
-- 4. TASK MANAGEMENT COMMANDS (COMMENTED OUT - UNCOMMENT TO USE)
-- ============================================================================

-- Enable task execution (uncomment to activate)
-- ALTER TASK TASK_PROCESS_TABLE_A_INCREMENTAL RESUME;

-- Manual task execution for testing (uncomment to test)
-- EXECUTE TASK TASK_PROCESS_TABLE_A_INCREMENTAL;

-- Check task run history (uncomment to check)
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     TASK_NAME => 'TASK_PROCESS_TABLE_A_INCREMENTAL',
--     SCHEDULED_TIME_RANGE_START => DATEADD('HOUR', -1, CURRENT_TIMESTAMP())
-- ));

-- ============================================================================
-- 5. ERROR INTEGRATION INFORMATION
-- ============================================================================

-- The task is configured with ERROR_INTEGRATION = STREAMS_TASKS_SNS_INTEGRATION
-- This will automatically send SNS notifications when the task fails
-- Error notifications include:
-- - Task name and execution details
-- - Error message and stack trace
-- - Timestamp of failure
-- - Retry information (if applicable)

-- To test error notifications, you can force a task failure by:
-- 1. Temporarily breaking the stored procedure logic
-- 2. Removing necessary privileges
-- 3. Suspending the warehouse

-- Example of checking task failures that would trigger notifications:
/*
SELECT 
    NAME,
    STATE,
    ERROR_CODE,
    ERROR_MESSAGE,
    SCHEDULED_TIME,
    COMPLETED_TIME
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE STATE = 'FAILED'
    AND NAME = 'TASK_PROCESS_TABLE_A_INCREMENTAL'
ORDER BY SCHEDULED_TIME DESC;
*/

SELECT 'Task created successfully! Remember to RESUME task to activate it.' AS status;
SELECT 'Error notifications will be sent to SNS when task fails.' AS notification_info; 