-- ============================================================================
-- Snowflake Streams and Tasks Implementation - Sample Tables Creation
-- ============================================================================
-- Purpose: Creates sample tables A and B with test data
-- Author: Generated Sample Code
-- Created: 2024
-- ============================================================================

-- Set context
USE ROLE STREAMS_TASKS_ROLE;
USE WAREHOUSE STREAMS_TASKS_WH;

-- ============================================================================
-- 1. CREATE TABLE A IN RAW_DB (SOURCE TABLE)
-- ============================================================================

USE DATABASE RAW_DB;
USE SCHEMA SAMPLE_DATA;

-- Drop table if exists (for testing purposes)
DROP TABLE IF EXISTS TABLE_A;

-- Create Table A with sample structure
CREATE TABLE TABLE_A (
    ID INTEGER AUTOINCREMENT START 1 INCREMENT 1,
    CUSTOMER_ID VARCHAR(50),
    TRANSACTION_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    AMOUNT DECIMAL(10,2),
    TRANSACTION_TYPE VARCHAR(20),
    STATUS VARCHAR(10) DEFAULT 'ACTIVE',
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Source table for raw transaction data';

-- ============================================================================
-- 2. CREATE TABLE B IN STAGING_DB (TARGET TABLE)
-- ============================================================================

USE DATABASE STAGING_DB;
USE SCHEMA PROCESSED_DATA;

-- Drop table if exists (for testing purposes)
DROP TABLE IF EXISTS TABLE_B;

-- Create Table B with enhanced structure for staging
CREATE TABLE TABLE_B (
    ID INTEGER,
    CUSTOMER_ID VARCHAR(50),
    TRANSACTION_DATE TIMESTAMP_NTZ,
    AMOUNT DECIMAL(10,2),
    TRANSACTION_TYPE VARCHAR(20),
    STATUS VARCHAR(10),
    CREATED_AT TIMESTAMP_NTZ,
    UPDATED_AT TIMESTAMP_NTZ,
    -- Additional staging columns
    PROCESSED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    BATCH_ID VARCHAR(50),
    SOURCE_SYSTEM VARCHAR(20) DEFAULT 'RAW_DB',
    -- Change tracking columns
    STREAM_ACTION VARCHAR(10), -- INSERT, UPDATE, DELETE
    STREAM_TIMESTAMP TIMESTAMP_NTZ
) COMMENT = 'Staging table for processed transaction data';

-- ============================================================================
-- 3. INSERT SAMPLE DATA INTO TABLE A
-- ============================================================================

USE DATABASE RAW_DB;
USE SCHEMA SAMPLE_DATA;

-- Insert initial sample data
INSERT INTO TABLE_A (CUSTOMER_ID, TRANSACTION_DATE, AMOUNT, TRANSACTION_TYPE)
VALUES 
    ('CUST001', '2024-01-15 10:30:00', 150.75, 'PURCHASE'),
    ('CUST002', '2024-01-15 11:45:00', 89.99, 'PURCHASE'),
    ('CUST003', '2024-01-15 14:20:00', 250.00, 'DEPOSIT'),
    ('CUST001', '2024-01-16 09:15:00', 45.50, 'WITHDRAWAL'),
    ('CUST004', '2024-01-16 16:30:00', 1200.00, 'DEPOSIT'),
    ('CUST002', '2024-01-17 12:00:00', 75.25, 'PURCHASE'),
    ('CUST005', '2024-01-17 15:45:00', 500.00, 'TRANSFER'),
    ('CUST003', '2024-01-18 10:10:00', 125.00, 'WITHDRAWAL'),
    ('CUST006', '2024-01-18 13:25:00', 350.75, 'PURCHASE'),
    ('CUST004', '2024-01-19 11:30:00', 200.00, 'TRANSFER');

-- ============================================================================
-- 4. VERIFICATION QUERIES
-- ============================================================================

-- Check Table A data
SELECT 'TABLE_A Record Count' AS metric, COUNT(*) AS value FROM RAW_DB.SAMPLE_DATA.TABLE_A
UNION ALL
SELECT 'TABLE_B Record Count' AS metric, COUNT(*) AS value FROM STAGING_DB.PROCESSED_DATA.TABLE_B;

-- Show sample data from Table A
SELECT 'Sample data from Table A:' AS info;
SELECT * FROM RAW_DB.SAMPLE_DATA.TABLE_A ORDER BY ID LIMIT 5;

-- Show Table B structure (should be empty initially)
SELECT 'Table B structure (empty initially):' AS info;
DESCRIBE TABLE STAGING_DB.PROCESSED_DATA.TABLE_B;

SELECT 'Sample tables created successfully!' AS status; 