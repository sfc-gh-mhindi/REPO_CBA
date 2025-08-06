-- ============================================================================
-- Snowflake Streams and Tasks Implementation - Infrastructure Setup
-- ============================================================================
-- Purpose: Sets up the infrastructure for Streams and Tasks data pipeline
-- Author: Generated Sample Code
-- Created: 2024
-- ============================================================================

-- Set context
USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- 1. CREATE DATABASES AND SCHEMAS
-- ============================================================================

-- Create RAW database for source data
CREATE DATABASE IF NOT EXISTS RAW_DB
    COMMENT = 'Database for raw/source data';

-- Create STAGING database for processed data
CREATE DATABASE IF NOT EXISTS STAGING_DB
    COMMENT = 'Database for staging/processed data';

-- Create schemas
CREATE SCHEMA IF NOT EXISTS RAW_DB.SAMPLE_DATA
    COMMENT = 'Schema for sample raw data tables';

CREATE SCHEMA IF NOT EXISTS STAGING_DB.PROCESSED_DATA
    COMMENT = 'Schema for processed staging tables';

-- ============================================================================
-- 2. CREATE WAREHOUSE FOR TASK EXECUTION
-- ============================================================================

CREATE WAREHOUSE IF NOT EXISTS STREAMS_TASKS_WH
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for executing streams and tasks operations';

-- ============================================================================
-- 3. CREATE ROLES FOR SECURITY
-- ============================================================================

-- Create custom role for streams and tasks operations
CREATE ROLE IF NOT EXISTS STREAMS_TASKS_ROLE
    COMMENT = 'Role for managing streams and tasks operations';

-- Grant necessary privileges to the role
GRANT USAGE ON WAREHOUSE STREAMS_TASKS_WH TO ROLE STREAMS_TASKS_ROLE;
GRANT OPERATE ON WAREHOUSE STREAMS_TASKS_WH TO ROLE STREAMS_TASKS_ROLE;

-- Grant database and schema privileges
GRANT USAGE ON DATABASE RAW_DB TO ROLE STREAMS_TASKS_ROLE;
GRANT USAGE ON SCHEMA RAW_DB.SAMPLE_DATA TO ROLE STREAMS_TASKS_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA RAW_DB.SAMPLE_DATA TO ROLE STREAMS_TASKS_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA RAW_DB.SAMPLE_DATA TO ROLE STREAMS_TASKS_ROLE;

GRANT USAGE ON DATABASE STAGING_DB TO ROLE STREAMS_TASKS_ROLE;
GRANT USAGE ON SCHEMA STAGING_DB.PROCESSED_DATA TO ROLE STREAMS_TASKS_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA STAGING_DB.PROCESSED_DATA TO ROLE STREAMS_TASKS_ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA STAGING_DB.PROCESSED_DATA TO ROLE STREAMS_TASKS_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA STAGING_DB.PROCESSED_DATA TO ROLE STREAMS_TASKS_ROLE;

-- Grant stream and task privileges
GRANT CREATE STREAM ON SCHEMA RAW_DB.SAMPLE_DATA TO ROLE STREAMS_TASKS_ROLE;
GRANT CREATE TASK ON SCHEMA STAGING_DB.PROCESSED_DATA TO ROLE STREAMS_TASKS_ROLE;

-- Grant role to SYSADMIN for management
GRANT ROLE STREAMS_TASKS_ROLE TO ROLE SYSADMIN;

-- ============================================================================
-- 4. VERIFICATION QUERIES
-- ============================================================================

-- Verify databases
SHOW DATABASES LIKE 'RAW_DB';
SHOW DATABASES LIKE 'STAGING_DB';

-- Verify schemas
SHOW SCHEMAS IN DATABASE RAW_DB;
SHOW SCHEMAS IN DATABASE STAGING_DB;

-- Verify warehouse
SHOW WAREHOUSES LIKE 'STREAMS_TASKS_WH';

-- Verify role
SHOW ROLES LIKE 'STREAMS_TASKS_ROLE';

SELECT 'Infrastructure setup completed successfully!' AS status; 