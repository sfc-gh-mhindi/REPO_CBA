-- ============================================================================
-- Snowflake Streams and Tasks Implementation - Cleanup Script
-- ============================================================================
-- Purpose: Clean up all created objects for the Streams and Tasks demo
-- Author: Generated Sample Code
-- Created: 2024
-- WARNING: This script will remove all demo objects. Use with caution!
-- ============================================================================

-- Set context
USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- 1. SUSPEND AND DROP TASKS
-- ============================================================================

-- Suspend task before dropping (to prevent errors)
ALTER TASK IF EXISTS STAGING_DB.PROCESSED_DATA.TASK_PROCESS_TABLE_A_INCREMENTAL SUSPEND;

-- Drop task
DROP TASK IF EXISTS STAGING_DB.PROCESSED_DATA.TASK_PROCESS_TABLE_A_INCREMENTAL;

SELECT 'Task suspended and dropped' AS cleanup_step;

-- ============================================================================
-- 2. DROP STORED PROCEDURES
-- ============================================================================

-- Drop stored procedure
DROP PROCEDURE IF EXISTS STAGING_DB.PROCESSED_DATA.SP_PROCESS_TABLE_A_INCREMENTAL();

SELECT 'Stored procedure dropped' AS cleanup_step;

-- ============================================================================
-- 3. DROP STREAMS
-- ============================================================================

-- Drop stream
DROP STREAM IF EXISTS RAW_DB.SAMPLE_DATA.STREAM_TABLE_A;

SELECT 'Streams dropped' AS cleanup_step;

-- ============================================================================
-- 4. DROP VIEWS
-- ============================================================================

-- Drop monitoring view
DROP VIEW IF EXISTS STAGING_DB.PROCESSED_DATA.V_PIPELINE_MONITORING;

SELECT 'Views dropped' AS cleanup_step;

-- ============================================================================
-- 5. DROP TABLES
-- ============================================================================

-- Drop tables
DROP TABLE IF EXISTS RAW_DB.SAMPLE_DATA.TABLE_A;
DROP TABLE IF EXISTS STAGING_DB.PROCESSED_DATA.TABLE_B;

SELECT 'Tables dropped' AS cleanup_step;

-- ============================================================================
-- 6. DROP SCHEMAS
-- ============================================================================

-- Drop schemas
DROP SCHEMA IF EXISTS RAW_DB.SAMPLE_DATA;
DROP SCHEMA IF EXISTS STAGING_DB.PROCESSED_DATA;

SELECT 'Schemas dropped' AS cleanup_step;

-- ============================================================================
-- 7. DROP DATABASES
-- ============================================================================

-- Drop databases
DROP DATABASE IF EXISTS RAW_DB;
DROP DATABASE IF EXISTS STAGING_DB;

SELECT 'Databases dropped' AS cleanup_step;

-- ============================================================================
-- 8. DROP WAREHOUSE
-- ============================================================================

-- Drop warehouse
DROP WAREHOUSE IF EXISTS STREAMS_TASKS_WH;

SELECT 'Warehouse dropped' AS cleanup_step;

-- ============================================================================
-- 9. DROP NOTIFICATION INTEGRATION
-- ============================================================================

-- Drop SNS notification integration
DROP INTEGRATION IF EXISTS STREAMS_TASKS_SNS_INTEGRATION;

SELECT 'Notification integration dropped' AS cleanup_step;

-- ============================================================================
-- 10. DROP ROLE (OPTIONAL - UNCOMMENT IF NEEDED)
-- ============================================================================

-- Revoke role from SYSADMIN first
-- REVOKE ROLE STREAMS_TASKS_ROLE FROM ROLE SYSADMIN;

-- Drop the custom role (uncomment if you want to remove it completely)
-- DROP ROLE IF EXISTS STREAMS_TASKS_ROLE;

-- SELECT 'Role dropped' AS cleanup_step;

-- ============================================================================
-- 11. VERIFICATION QUERIES
-- ============================================================================

-- Verify cleanup
SELECT 'Cleanup Verification:' AS info;

-- Check if any objects still exist
SELECT 'Remaining databases:' AS check_type;
SHOW DATABASES LIKE 'RAW_DB';
SHOW DATABASES LIKE 'STAGING_DB';

SELECT 'Remaining warehouses:' AS check_type;
SHOW WAREHOUSES LIKE 'STREAMS_TASKS_WH';

SELECT 'Remaining roles:' AS check_type;
SHOW ROLES LIKE 'STREAMS_TASKS_ROLE';

-- ============================================================================
-- 12. FINAL STATUS
-- ============================================================================

SELECT 
    'CLEANUP COMPLETED' AS status,
    'All demo objects have been removed' AS message,
    CURRENT_TIMESTAMP() AS cleanup_time; 