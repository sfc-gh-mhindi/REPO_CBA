-- =====================================================================
-- Process Key Generator Procedure (Converted from BTEQ)
-- Original BTEQ: sp_get_pros_key.sql
-- =====================================================================

-- Version History:
-- Ver  Date       Modified By            Description
-- ---- ---------- ---------------------- -----------------------------------
-- 1.0  11/06/2013 T Jelliffe             Initial Version
-- 2.0  2025-01-XX Cursor AI              Converted to Snowflake

-- Description: Generate process keys for batch processing

CREATE OR REPLACE PROCEDURE SP_GET_PROS_KEY_WRAPPER(
    ENV_CODE STRING,
    STREAM_CODE STRING,
    TABLE_SHORT STRING,
    GDW_USER STRING,
    SOURCE_SYSTEM_M STRING,
    SOURCE_M STRING,
    PSST_TABLE_M STRING,
    INPUT_DATE DATE,
    RESTART_FLAG STRING DEFAULT 'N',
    PLATFORM STRING DEFAULT 'MVS'
)
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Snowflake equivalent of BTEQ process key generation workflow'
AS
$$
DECLARE
    batch_key STRING;
    process_key STRING;
    result_msg STRING;
    file_path STRING;
    batch_key_table STRING;
    process_key_table STRING;
BEGIN
    -- Construct table names for batch and process keys (Snowflake equivalent of file operations)
    batch_key_table := STREAM_CODE || '_BTCH_KEY_TEMP';
    process_key_table := STREAM_CODE || '_' || TABLE_SHORT || 'PROS_KEY_TEMP';
    
    -- Read batch key from temporary table (equivalent to IMPORT from file)
    -- Note: In Snowflake, file operations are replaced with staging tables
    SELECT BTCH_KEY 
    INTO batch_key
    FROM IDENTIFIER(batch_key_table)
    LIMIT 1;
    
    -- Call the stored procedure to get process key
    -- Snowflake equivalent of the BTEQ CALL statement
    CALL SP_GET_PROS_KEY(
        GDW_USER,
        SOURCE_SYSTEM_M,
        SOURCE_M,
        PSST_TABLE_M,
        TRY_CAST(TRIM(batch_key) AS NUMBER(10,0)),
        INPUT_DATE,
        RESTART_FLAG,
        PLATFORM,
        process_key  -- Output parameter
    );
    
    -- Store process key in temporary table (equivalent to EXPORT to file)
    CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(process_key_table) AS
    SELECT process_key AS PROCESS_KEY;
    
    result_msg := 'Process key generated successfully: ' || process_key;
    RETURN result_msg;
    
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error in SP_GET_PROS_KEY_WRAPPER: ' || SQLERRM;
END;
$$;

-- Alternative implementation using Snowflake stages for file operations
CREATE OR REPLACE PROCEDURE SP_GET_PROS_KEY_WITH_STAGES(
    STAGE_NAME STRING,
    ENV_CODE STRING,
    STREAM_CODE STRING,
    TABLE_SHORT STRING,
    -- ... other parameters ...
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    batch_key STRING;
    process_key STRING;
BEGIN
    -- Read from stage (Snowflake equivalent of file import)
    SELECT $1
    INTO batch_key
    FROM '@' || STAGE_NAME || '/' || STREAM_CODE || '_BTCH_KEY.txt'
    (FILE_FORMAT => (TYPE = 'CSV', FIELD_DELIMITER = '\t'))
    LIMIT 1;
    
    -- Generate process key and write to stage
    CALL SP_GET_PROS_KEY(/* parameters */);
    
    -- Write result to stage (equivalent of EXPORT)
    COPY INTO '@' || STAGE_NAME || '/' || STREAM_CODE || '_' || TABLE_SHORT || 'PROS_KEY.txt'
    FROM (SELECT process_key)
    FILE_FORMAT = (TYPE = 'CSV');
    
    RETURN 'Success';
END;
$$;

-- Usage examples:
-- CALL SP_GET_PROS_KEY_WRAPPER('DEV', 'ACCT', 'BAL', 'user', 'SYS', 'SRC', 'TABLE', CURRENT_DATE());
-- CALL SP_GET_PROS_KEY_WITH_STAGES('MY_STAGE', 'DEV', 'ACCT', 'BAL'); 