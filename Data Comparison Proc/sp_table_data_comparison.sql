USE ROLE R_DEV_NPD_D12_GDWMIG;
USE WAREHOUSE WH_USR_NPD_D12_GDWMIG_022;
use database NPD_D12_DMN_GDWMIG;
USE SCHEMA NPD_D12_DMN_GDWMIG.TMP;

 
-- =====================================================================================
-- Stored Procedure: SP_TABLE_DATA_COMPARISON
-- Purpose: Comprehensive data comparison between two tables/views (source vs target)
-- Supports: Iceberg tables, Native Snowflake tables, Views, Mixed comparisons
-- Author: Data Engineering Team
-- Version: 2.1 - Simplified for Snowflake Compatibility
-- =====================================================================================
-- drop  PROCEDURE SP_TABLE_DATA_COMPARISON(varchar, varchar, varchar, varchar, varchar, varchar, varchar, varchar);
CREATE OR REPLACE PROCEDURE SP_TABLE_DATA_COMPARISON(

    -- Source table/view parameters
    SOURCE_DATABASE VARCHAR(255),
    SOURCE_SCHEMA VARCHAR(255), 
    SOURCE_TABLE VARCHAR(255),

    -- Target table/view parameters
    TARGET_DATABASE VARCHAR(255),
    TARGET_SCHEMA VARCHAR(255),
    TARGET_TABLE VARCHAR(255),

    -- Comparison options
    COMPARISON_KEY_COLUMNS VARCHAR(1000) DEFAULT NULL

)

RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE

    -- Variables for dynamic SQL

    source_full_name VARCHAR(1000);
    target_full_name VARCHAR(1000);
    comparison_result VARCHAR(16777216) DEFAULT '';
  
    
    -- Comparison metrics
    source_row_count NUMBER;
    target_row_count NUMBER;

    
    -- Schema comparison
    schema_diff_count NUMBER;
    missing_in_target NUMBER;
    missing_in_source NUMBER;
    data_type_mismatches NUMBER;
  
    
    -- Data comparison
    data_matches NUMBER;
    data_total NUMBER;
    data_mismatches NUMBER;
    missing_in_source_count NUMBER;
    missing_in_target_count NUMBER;
    
    -- Execution tracking

    start_time TIMESTAMP_LTZ;
    end_time TIMESTAMP_LTZ;
    execution_duration VARCHAR(100);

    
    -- Error handling
    error_message VARCHAR(5000);
    
BEGIN
    -- Initialize execution tracking

    start_time := CURRENT_TIMESTAMP();
    
    -- Build full table names

    source_full_name := SOURCE_DATABASE || '.' || SOURCE_SCHEMA || '.' || SOURCE_TABLE;
    target_full_name := TARGET_DATABASE || '.' || TARGET_SCHEMA || '.' || TARGET_TABLE;
    
    comparison_result := '=== TABLE/VIEW DATA COMPARISON REPORT ===' || CHR(10) || CHR(10);
    comparison_result := comparison_result || 'Execution Started: ' || start_time::VARCHAR || CHR(10);
    comparison_result := comparison_result || 'Source Object: ' || source_full_name || CHR(10);
    comparison_result := comparison_result || 'Target Object: ' || target_full_name || CHR(10);
    comparison_result := comparison_result || 'Comparison Type: FULL DATA COMPARISON' || CHR(10);
    comparison_result := comparison_result || REPEAT('-', 80) || CHR(10) || CHR(10);
    
    -- =================================================================================
    -- STEP 1: OBJECT EXISTENCE CHECK
    -- =================================================================================
    comparison_result := comparison_result || '1. OBJECT EXISTENCE CHECK' || CHR(10);
    comparison_result := comparison_result || REPEAT('-', 30) || CHR(10);
    
    BEGIN
        -- Check source object exists and is accessible
        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || source_full_name || ' LIMIT 1';
        comparison_result := comparison_result || '✓ Source object exists and is accessible' || CHR(10);
    EXCEPTION
        WHEN OTHER THEN
            comparison_result := comparison_result || '✗ ERROR: Source object not found or not accessible: ' || SQLERRM || CHR(10);
            RETURN comparison_result;
    END;
    
    BEGIN
        -- Check target object exists and is accessible
        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || target_full_name || ' LIMIT 1';
        comparison_result := comparison_result || '✓ Target object exists and is accessible' || CHR(10) || CHR(10);
    EXCEPTION
        WHEN OTHER THEN
            comparison_result := comparison_result || '✗ ERROR: Target object not found or not accessible: ' || SQLERRM || CHR(10);
            RETURN comparison_result;
    END;
    -- =================================================================================
    -- STEP 2: ROW COUNT COMPARISON
    -- =================================================================================
    comparison_result := comparison_result || '2. ROW COUNT COMPARISON' || CHR(10);
    comparison_result := comparison_result || REPEAT('-', 30) || CHR(10);

    
    -- Get source row count

    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || source_full_name;
    select $1 into source_row_count from table(result_scan(last_query_id()));
  
    
    -- Get target row count
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || target_full_name;
    select $1 into target_row_count from table(result_scan(last_query_id()));
    
    comparison_result := comparison_result || 'Source Row Count: ' || source_row_count::VARCHAR || CHR(10);
    comparison_result := comparison_result || 'Target Row Count: ' || target_row_count::VARCHAR || CHR(10);
    comparison_result := comparison_result || 'Row Count Difference: ' || (target_row_count - source_row_count)::VARCHAR || CHR(10);
    
    IF (source_row_count = target_row_count) THEN
        comparison_result := comparison_result || '✓ Row counts match perfectly' || CHR(10) || CHR(10);
    ELSE
        comparison_result := comparison_result || '⚠ Row counts do not match' || CHR(10) || CHR(10);
    END IF;
    
    -- =================================================================================
    -- STEP 3: SCHEMA COMPARISON
    -- =================================================================================
    comparison_result := comparison_result || '3. SCHEMA COMPARISON' || CHR(10);
    comparison_result := comparison_result || REPEAT('-', 30) || CHR(10);
    
    -- Create schema comparison
    EXECUTE IMMEDIATE '
    CREATE OR REPLACE TEMPORARY TABLE temp_schema_comparison AS
    WITH source_schema AS (
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            ORDINAL_POSITION,
            ''SOURCE'' as table_source
        FROM ' || SOURCE_DATABASE || '.INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = ''' || SOURCE_SCHEMA || ''' 
          AND TABLE_NAME = ''' || SOURCE_TABLE || '''
    ),
    target_schema AS (
        SELECT 
            COLUMN_NAME,
            DATA_TYPE, 
            IS_NULLABLE,
            ORDINAL_POSITION,
            ''TARGET'' as table_source
        FROM ' || TARGET_DATABASE || '.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ''' || TARGET_SCHEMA || '''
          AND TABLE_NAME = ''' || TARGET_TABLE || '''
    ),
    schema_comparison AS (
        SELECT 
            COALESCE(s.COLUMN_NAME, t.COLUMN_NAME) as COLUMN_NAME,
            s.DATA_TYPE as SOURCE_DATA_TYPE,
            t.DATA_TYPE as TARGET_DATA_TYPE,
            s.IS_NULLABLE as SOURCE_NULLABLE,
            t.IS_NULLABLE as TARGET_NULLABLE,
            s.ORDINAL_POSITION as SOURCE_POSITION,
            t.ORDINAL_POSITION as TARGET_POSITION,
                         CASE 
                 WHEN s.COLUMN_NAME IS NULL THEN ''MISSING_IN_SOURCE''
                 WHEN t.COLUMN_NAME IS NULL THEN ''MISSING_IN_TARGET''
                 -- Handle Iceberg vs Native table data type differences
                 WHEN s.DATA_TYPE != t.DATA_TYPE AND NOT (
                     -- Common Iceberg/Native equivalents
                     (s.DATA_TYPE = ''VARCHAR'' AND t.DATA_TYPE LIKE ''VARCHAR%'') OR
                     (t.DATA_TYPE = ''VARCHAR'' AND s.DATA_TYPE LIKE ''VARCHAR%'') OR
                     (s.DATA_TYPE = ''NUMBER'' AND t.DATA_TYPE LIKE ''NUMBER%'') OR
                     (t.DATA_TYPE = ''NUMBER'' AND s.DATA_TYPE LIKE ''NUMBER%'') OR
                     (s.DATA_TYPE = ''TIMESTAMP_NTZ'' AND t.DATA_TYPE = ''TIMESTAMP_NTZ(6)'') OR
                     (t.DATA_TYPE = ''TIMESTAMP_NTZ'' AND s.DATA_TYPE = ''TIMESTAMP_NTZ(6)'') OR
                     (s.DATA_TYPE = ''TIMESTAMP_LTZ'' AND t.DATA_TYPE = ''TIMESTAMP_LTZ(6)'') OR
                     (t.DATA_TYPE = ''TIMESTAMP_LTZ'' AND s.DATA_TYPE = ''TIMESTAMP_LTZ(6)'') OR
                     (s.DATA_TYPE = ''TIME'' AND t.DATA_TYPE = ''TIME(6)'') OR
                     (t.DATA_TYPE = ''TIME'' AND s.DATA_TYPE = ''TIME(6)'')
                 ) THEN ''DATA_TYPE_MISMATCH''
                 WHEN s.IS_NULLABLE != t.IS_NULLABLE THEN ''NULLABLE_MISMATCH''
                 WHEN s.ORDINAL_POSITION != t.ORDINAL_POSITION THEN ''POSITION_MISMATCH''
                 ELSE ''MATCH''
             END as COMPARISON_STATUS
        FROM source_schema s
        FULL OUTER JOIN target_schema t ON s.COLUMN_NAME = t.COLUMN_NAME
    )
    SELECT * FROM schema_comparison
    ORDER BY COALESCE(SOURCE_POSITION, TARGET_POSITION)';
    
    -- Get schema comparison metrics
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM temp_schema_comparison WHERE COMPARISON_STATUS = ''MISSING_IN_TARGET''';
    select $1 into missing_in_target from table(result_scan(last_query_id()));
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM temp_schema_comparison WHERE COMPARISON_STATUS = ''MISSING_IN_SOURCE''';
    select $1 into missing_in_source from table(result_scan(last_query_id()));
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM temp_schema_comparison WHERE COMPARISON_STATUS = ''DATA_TYPE_MISMATCH''';
    select $1 into data_type_mismatches from table(result_scan(last_query_id()));
    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM temp_schema_comparison WHERE COMPARISON_STATUS != ''MATCH''';
    select $1 into schema_diff_count from table(result_scan(last_query_id()));
    
    comparison_result := comparison_result || 'Schema Differences Found: ' || schema_diff_count::VARCHAR || CHR(10);
    comparison_result := comparison_result || 'Columns Missing in Target: ' || missing_in_target::VARCHAR || CHR(10);
    comparison_result := comparison_result || 'Columns Missing in Source: ' || missing_in_source::VARCHAR || CHR(10);
    comparison_result := comparison_result || 'Data Type Mismatches: ' || data_type_mismatches::VARCHAR || CHR(10);
    
    IF (schema_diff_count = 0) THEN
        comparison_result := comparison_result || '✓ Schemas match perfectly' || CHR(10) || CHR(10);
    ELSE
        comparison_result := comparison_result || '✓ Schemas do not match' || CHR(10) || CHR(10);
        -- comparison_result := comparison_result || '⚠ Schema differences detected' || CHR(10);
        
        -- -- Add detailed schema differences

        -- FOR record IN (SELECT COLUMN_NAME, COMPARISON_STATUS, SOURCE_DATA_TYPE, TARGET_DATA_TYPE
        --               FROM temp_schema_comparison
        --               WHERE COMPARISON_STATUS != 'MATCH'
        --               ORDER BY COLUMN_NAME) DO
        --     comparison_result := comparison_result || '  - ' || record.COLUMN_NAME || ': ' || record.COMPARISON_STATUS;
        --     IF (record.COMPARISON_STATUS = 'DATA_TYPE_MISMATCH') THEN
        --         comparison_result := comparison_result || ' (' || COALESCE(record.SOURCE_DATA_TYPE, 'NULL') || ' vs ' || COALESCE(record.TARGET_DATA_TYPE, 'NULL') || ')';
        --     END IF;
        --     comparison_result := comparison_result || CHR(10);
        -- END FOR;
        comparison_result := comparison_result || CHR(10);
    END IF;
    
    -- =================================================================================
    -- STEP 4: FULL DATA COMPARISON
    -- =================================================================================
    comparison_result := comparison_result || '4. FULL DATA COMPARISON' || CHR(10);
    comparison_result := comparison_result || REPEAT('-', 30) || CHR(10);
    
    -- Only proceed with data comparison if schemas are compatible
    IF (schema_diff_count = 0) THEN
        -- Use comparison key columns if provided for row-level comparison
        IF (COMPARISON_KEY_COLUMNS IS NOT NULL AND TRIM(COMPARISON_KEY_COLUMNS) != '') THEN
            -- Create full data comparison using business keys
            EXECUTE IMMEDIATE '
            CREATE OR REPLACE TEMPORARY TABLE temp_data_comparison AS
            WITH source_data AS (
                SELECT SHA2(COALESCE(CONCAT_WS(''|'', ' || COMPARISON_KEY_COLUMNS || '), ''NULL_KEY''), 256) as row_key,
                       SHA2(COALESCE(CONCAT_WS(''|'', *), ''NULL_ROW''), 256) as full_row_hash,
                       ''SOURCE'' as data_source
                FROM ' || source_full_name || '
            ),
            target_data AS (
                SELECT SHA2(COALESCE(CONCAT_WS(''|'', ' || COMPARISON_KEY_COLUMNS || '), ''NULL_KEY''), 256) as row_key,
                       SHA2(COALESCE(CONCAT_WS(''|'', *), ''NULL_ROW''), 256) as full_row_hash,
                       ''TARGET'' as data_source
                FROM ' || target_full_name || '
            )
            SELECT 
                COALESCE(s.row_key, t.row_key) as row_key,
                s.full_row_hash as source_hash,
                t.full_row_hash as target_hash,
                CASE 
                    WHEN s.row_key IS NULL THEN ''MISSING_IN_SOURCE''
                    WHEN t.row_key IS NULL THEN ''MISSING_IN_TARGET''
                    WHEN s.full_row_hash = t.full_row_hash THEN ''MATCH''
                    ELSE ''DATA_MISMATCH''
                END as comparison_status
            FROM source_data s
            FULL OUTER JOIN target_data t ON s.row_key = t.row_key';
            
            -- Get detailed comparison metrics
            EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM temp_data_comparison WHERE comparison_status = ''MATCH''';
            select $1 into data_matches from table(result_scan(last_query_id()));
            EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM temp_data_comparison WHERE comparison_status = ''DATA_MISMATCH''';
            select $1 into data_mismatches from table(result_scan(last_query_id()));
            EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM temp_data_comparison WHERE comparison_status = ''MISSING_IN_SOURCE''';
            select $1 into missing_in_source_count from table(result_scan(last_query_id()));
            EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM temp_data_comparison WHERE comparison_status = ''MISSING_IN_TARGET''';
            select $1 into missing_in_target_count from table(result_scan(last_query_id()));
            EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM temp_data_comparison';
            select $1 into data_total from table(result_scan(last_query_id()));
            
        ELSE
            -- Row-by-row hash comparison when no key columns specified
            EXECUTE IMMEDIATE '
            CREATE OR REPLACE TEMPORARY TABLE temp_data_comparison AS
            WITH source_data AS (
                SELECT 
                    ROW_NUMBER() OVER (ORDER BY 1) as rn,
                    SHA2(COALESCE(CONCAT_WS(''|'', *), ''NULL_ROW''), 256) as row_hash
                FROM ' || source_full_name || '
            ),
            target_data AS (
                SELECT 
                    ROW_NUMBER() OVER (ORDER BY 1) as rn,
                    SHA2(COALESCE(CONCAT_WS(''|'', *), ''NULL_ROW''), 256) as row_hash
                FROM ' || target_full_name || '
            )
            SELECT 
                COALESCE(s.rn, t.rn) as row_number,
                s.row_hash as source_hash,
                t.row_hash as target_hash,
                CASE 
                    WHEN s.rn IS NULL THEN ''MISSING_IN_SOURCE''
                    WHEN t.rn IS NULL THEN ''MISSING_IN_TARGET''
                    WHEN s.row_hash = t.row_hash THEN ''MATCH''
                    ELSE ''DATA_MISMATCH''
                END as comparison_status
            FROM source_data s
            FULL OUTER JOIN target_data t ON s.rn = t.rn';
            
            -- Get comparison metrics
            EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM temp_data_comparison WHERE comparison_status = ''MATCH''';
            select $1 into data_matches from table(result_scan(last_query_id()));
            EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM temp_data_comparison WHERE comparison_status = ''DATA_MISMATCH''';
            select $1 into data_mismatches from table(result_scan(last_query_id()));
            EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM temp_data_comparison WHERE comparison_status = ''MISSING_IN_SOURCE''';
            select $1 into missing_in_source_count from table(result_scan(last_query_id()));
            EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM temp_data_comparison WHERE comparison_status = ''MISSING_IN_TARGET''';
            select $1 into missing_in_target_count from table(result_scan(last_query_id()));
            EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM temp_data_comparison';
            select $1 into data_total from table(result_scan(last_query_id()));
        END IF;
            
            comparison_result := comparison_result || 'Total Rows Compared: ' || data_total::VARCHAR || CHR(10);
            comparison_result := comparison_result || 'Matching Rows: ' || data_matches::VARCHAR || CHR(10);
            comparison_result := comparison_result || 'Data Mismatches: ' || data_mismatches::VARCHAR || CHR(10);
            comparison_result := comparison_result || 'Missing in Source: ' || missing_in_source_count::VARCHAR || CHR(10);
            comparison_result := comparison_result || 'Missing in Target: ' || missing_in_target_count::VARCHAR || CHR(10);
            comparison_result := comparison_result || 'Match Percentage: ' || ROUND((data_matches::FLOAT / data_total::FLOAT) * 100, 2)::VARCHAR || '%' || CHR(10);
        
        IF (data_matches = data_total) THEN
            comparison_result := comparison_result || '✓ All data matches perfectly' || CHR(10) || CHR(10);
        ELSE
            comparison_result := comparison_result || '⚠ Data differences detected' || CHR(10) || CHR(10);
        END IF;
        
    ELSE
        comparison_result := comparison_result || '⚠ Skipping data comparison due to schema differences' || CHR(10) || CHR(10);
    END IF;
    
    -- =================================================================================
    -- STEP 5: SUMMARY AND RECOMMENDATIONS
    -- =================================================================================
    end_time := CURRENT_TIMESTAMP();
    execution_duration := DATEDIFF('second', start_time, end_time)::VARCHAR || ' seconds';

    
    comparison_result := comparison_result || '5. SUMMARY AND RECOMMENDATIONS' || CHR(10);
    comparison_result := comparison_result || REPEAT('-', 40) || CHR(10);
    comparison_result := comparison_result || 'Execution Duration: ' || execution_duration || CHR(10);
    comparison_result := comparison_result || 'Execution Completed: ' || end_time::VARCHAR || CHR(10) || CHR(10);
    
    -- Overall assessment
    IF (source_row_count = target_row_count AND schema_diff_count = 0 AND (data_matches = data_total OR data_total = 0)) THEN
        comparison_result := comparison_result || '🎉 OVERALL RESULT: TABLES ARE IDENTICAL' || CHR(10);
        comparison_result := comparison_result || '   All checks passed successfully - data migration verified' || CHR(10);
    ELSE
        comparison_result := comparison_result || '⚠️  OVERALL RESULT: DIFFERENCES DETECTED' || CHR(10);
        comparison_result := comparison_result || '   Review the detailed comparison above' || CHR(10);

        
        -- Recommendations
        comparison_result := comparison_result || CHR(10) || 'RECOMMENDATIONS:' || CHR(10);
        IF (source_row_count != target_row_count) THEN
            comparison_result := comparison_result || '• Investigate row count discrepancy (' || (target_row_count - source_row_count)::VARCHAR || ' difference)' || CHR(10);
        END IF;

        IF (schema_diff_count > 0) THEN
            comparison_result := comparison_result || '• Resolve schema differences before data migration' || CHR(10);
        END IF;

        IF (data_matches != data_total AND data_total > 0) THEN
            comparison_result := comparison_result || '• Investigate ' || data_mismatches::VARCHAR || ' data mismatches' || CHR(10);
            IF (missing_in_source_count > 0) THEN
                comparison_result := comparison_result || '• Check why ' || missing_in_source_count::VARCHAR || ' records are missing in source' || CHR(10);
            END IF;

            IF (missing_in_target_count > 0) THEN
                comparison_result := comparison_result || '• Check why ' || missing_in_target_count::VARCHAR || ' records are missing in target' || CHR(10);
            END IF;
        END IF;
    END IF;
    
    comparison_result := comparison_result || CHR(10) || REPEAT('=', 80);
    
    -- Cleanup temporary objects

    EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS temp_schema_comparison';
    EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS temp_data_comparison';
    
    RETURN comparison_result;
    
EXCEPTION
    WHEN OTHER THEN
        error_message := 'ERROR in SP_TABLE_DATA_COMPARISON: ' || SQLERRM;
        comparison_result := comparison_result || CHR(10) || CHR(10) || error_message;
        
        -- Cleanup on error
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS temp_schema_comparison';
            EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS temp_data_comparison';
        EXCEPTION
            WHEN OTHER THEN NULL; -- Ignore cleanup errors
        END;
        
        RETURN comparison_result;
END;

$$;

 

-- =====================================================================================
-- USAGE EXAMPLES
-- =====================================================================================

/*
IMPORTANT: Primary key columns must be passed with single quotes around each column name!

-- Example 1: Basic comparison between two tables (full data comparison, no keys)
CALL SP_TABLE_DATA_COMPARISON(
    'PSUND_MIGR_DCF', 'P_D_DCF_001_STD_0', 'BV_PDS_TRAN',  -- Source
    'PSUND_MIGR_CLD', 'PDBAL001STD0', 'BV_PDS_TRAN',        -- Target
    NULL                                                     -- No key columns (row-by-row comparison)
);

-- Example 2: Single primary key column (CORRECT FORMAT)
CALL SP_TABLE_DATA_COMPARISON(
    'NPD_D12_DMN_GDWMIG', 'TMP', 'SOURCE_TABLE',            -- Source
    'NPD_D12_DMN_GDWMIG', 'TMP', 'TARGET_TABLE',            -- Target
    '''TRAN_RCPT_N'''                                       -- Single key with quotes
);

-- Example 3: Composite primary key (CORRECT FORMAT)
CALL SP_TABLE_DATA_COMPARISON(
    'PSUND_MIGR_DCF', 'P_D_DCF_001_STD_0', 'BV_PDS_TRAN',  -- Source
    'PSUND_MIGR_CLD', 'PDBAL001STD0', 'BV_PDS_TRAN',        -- Target
    '''TRAN_RCPT_N'',''INDS_TRAN_I'''                       -- Composite key with quotes
);

-- Example 4: Three-column composite key (CORRECT FORMAT)
CALL SP_TABLE_DATA_COMPARISON(
    'DATABASE_A', 'SCHEMA_A', 'TABLE_A',                    -- Source
    'DATABASE_B', 'SCHEMA_B', 'TABLE_B',                    -- Target
    '''COL1'',''COL2'',''COL3'''                            -- Three-column key with quotes
);

-- Example 5: Mixed comparison (table vs view)
CALL SP_TABLE_DATA_COMPARISON(
    'PROD_DB', 'CORE_SCHEMA', 'TRANSACTIONS_TABLE',         -- Source table
    'REPORTING_DB', 'VIEW_SCHEMA', 'TRANSACTIONS_VIEW',     -- Target view
    '''TRANSACTION_ID'',''ACCOUNT_ID'''                     -- Key columns with quotes
);

-- WRONG FORMAT (will cause errors or show all mismatches):
-- 'TRAN_RCPT_N,INDS_TRAN_I'      -- Missing quotes around individual columns
-- '''TRAN_RCPT_N,INDS_TRAN_I'''  -- Missing quotes around individual columns
-- NULL or empty string            -- Will do row-by-row comparison (slow, shows all mismatches)

-- CORRECT FORMAT:
-- '''TRAN_RCPT_N'',''INDS_TRAN_I'''  -- Each column quoted separately
*/

 

-- =====================================================================================

-- VIEW-SPECIFIC CONSIDERATIONS AND BEST PRACTICES

-- =====================================================================================

 

/*

IMPORTANT NOTES FOR VIEW COMPARISONS:

 

1. PERFORMANCE IMPACT:

   - Views execute underlying queries each time they're accessed

   - Complex views with multiple JOINs can be very slow

   - Consider using ENABLE_VIEW_OPTIMIZATIONS=FALSE for very complex views

 

2. SCHEMA DETECTION:

   - Some views may not appear correctly in INFORMATION_SCHEMA.COLUMNS

   - The procedure includes fallback mechanisms for schema detection

   - Computed columns in views may show different data types

 

3. RECOMMENDED USAGE PATTERNS:

  

   For DBT Model vs Teradata View comparison:

   - Always specify key columns for better performance

   - Exclude audit/timestamp columns that may differ

   - Use during development and testing phases

  

   For View vs View comparison:

   - Ensure both views have similar complexity

   - Consider running during off-peak hours

   - Monitor execution time and resource usage

 

4. TROUBLESHOOTING:

   - If schema comparison fails, check view definitions

   - For timeout issues, try with smaller sample sizes first

   - Use key-based comparison for large datasets

 

5. OPTIMIZATION TIPS:

   - Create temporary tables from views for repeated comparisons

   - Use filtered comparisons with WHERE clauses when possible

   - Consider materialized views for frequently compared complex views

*/ 