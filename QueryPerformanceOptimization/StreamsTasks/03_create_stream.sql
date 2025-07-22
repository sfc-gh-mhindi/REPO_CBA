-- ============================================================================
-- Snowflake Streams and Tasks Implementation - Stream Creation
-- ============================================================================
-- Purpose: Creates a stream on Table A to capture data changes
-- Author: Generated Sample Code
-- Created: 2024
-- ============================================================================

-- Set context
USE ROLE STREAMS_TASKS_ROLE;
USE WAREHOUSE STREAMS_TASKS_WH;
USE DATABASE RAW_DB;
USE SCHEMA SAMPLE_DATA;

-- ============================================================================
-- 1. CREATE STREAM ON TABLE A
-- ============================================================================

-- Drop stream if exists (for testing purposes)
DROP STREAM IF EXISTS STREAM_TABLE_A;

-- Create stream to capture changes on Table A
CREATE STREAM STREAM_TABLE_A 
ON TABLE RAW_DB.SAMPLE_DATA.TABLE_A
COMMENT = 'Stream to capture all changes (INSERT, UPDATE, DELETE) on Table A';

-- ============================================================================
-- 2. VERIFY STREAM CREATION
-- ============================================================================

-- Show streams in the schema
SHOW STREAMS IN SCHEMA RAW_DB.SAMPLE_DATA;

-- Check stream metadata
SELECT 
    STREAM_NAME,
    TABLE_NAME,
    OWNER,
    COMMENT,
    CREATED_ON,
    MODE,
    STALE,
    STALE_AFTER
FROM RAW_DB.INFORMATION_SCHEMA.STREAMS 
WHERE STREAM_NAME = 'STREAM_TABLE_A';

-- ============================================================================
-- 3. INITIAL STREAM CONTENT CHECK
-- ============================================================================

-- Check initial stream content (should be empty since no changes after stream creation)
SELECT 'Initial stream content (should be empty):' AS info;
SELECT COUNT(*) AS initial_stream_record_count FROM RAW_DB.SAMPLE_DATA.STREAM_TABLE_A;

-- Show stream structure
SELECT 'Stream structure:' AS info;
DESCRIBE STREAM RAW_DB.SAMPLE_DATA.STREAM_TABLE_A;

-- ============================================================================
-- 4. TEST STREAM FUNCTIONALITY
-- ============================================================================

-- Insert a test record to see if stream captures it
INSERT INTO RAW_DB.SAMPLE_DATA.TABLE_A (CUSTOMER_ID, TRANSACTION_DATE, AMOUNT, TRANSACTION_TYPE)
VALUES ('CUST_TEST', CURRENT_TIMESTAMP(), 999.99, 'TEST');

-- Check stream content after insert
SELECT 'Stream content after test insert:' AS info;
SELECT * FROM RAW_DB.SAMPLE_DATA.STREAM_TABLE_A;

-- Update a record to test UPDATE capture
UPDATE RAW_DB.SAMPLE_DATA.TABLE_A 
SET AMOUNT = 111.11, UPDATED_AT = CURRENT_TIMESTAMP()
WHERE CUSTOMER_ID = 'CUST_TEST';

-- Check stream content after update
SELECT 'Stream content after test update:' AS info;
SELECT 
    METADATA$ACTION,
    METADATA$ISUPDATE,
    METADATA$ROW_ID,
    ID,
    CUSTOMER_ID,
    AMOUNT,
    TRANSACTION_TYPE,
    UPDATED_AT
FROM RAW_DB.SAMPLE_DATA.STREAM_TABLE_A
ORDER BY METADATA$ACTION, ID;

-- ============================================================================
-- 5. STREAM INFORMATION QUERIES
-- ============================================================================

-- Get stream statistics
SELECT 
    'Stream Statistics' AS category,
    'STREAM_TABLE_A' AS stream_name,
    (SELECT COUNT(*) FROM RAW_DB.SAMPLE_DATA.STREAM_TABLE_A) AS current_changes,
    (SELECT COUNT(*) FROM RAW_DB.SAMPLE_DATA.TABLE_A) AS total_table_records;

SELECT 'Stream created and tested successfully!' AS status; 