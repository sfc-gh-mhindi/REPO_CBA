-- =====================================================================
-- Portfolio Technical Interest Processing (Converted from BTEQ)
-- Original BTEQ: prtf_tech_int_psst.sql
-- =====================================================================

-- Version History:
-- Ver  Date       Modified By        Description
-- ---- ---------- ------------------ ---------------------------------------
-- 1.0  11/06/2013 T Jelliffe         Initial Version
-- 1.1  11/07/2013 T Jelliffe         Use PROS_KEY_EFFT_I prevent self join
-- 1.2  12/07/2013 T Jelliffe         Time period reduced 15 to 3 years
-- 1.3  27/08/2013 T Jelliffe         Use only records with same EFFT_D
-- 1.4  21/10/2013 T Jelliffe         Insert/Delete changed records
-- 1.5  01/11/2013 T Jelliffe         Bug fixes - Row_Number not Rank
-- 2.0  2025-01-XX Cursor AI          Converted to Snowflake

-- Description: Process portfolio technical interest data with overlap detection

CREATE OR REPLACE PROCEDURE PRTF_TECH_INT_PSST_PROC(
    STAR_DATABASE STRING DEFAULT 'STARDATADB',
    VTECH_DATABASE STRING DEFAULT 'VTECH'
)
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Process portfolio technical interest data with deduplication'
AS
$$
DECLARE
    rows_deleted INTEGER := 0;
    rows_inserted INTEGER := 0;
    result_msg STRING;
BEGIN
    -- STEP 4: Delete all the original overlap records
    -- Note: Snowflake doesn't have native OVERLAPS operator, using custom UDF
    DELETE FROM IDENTIFIER(STAR_DATABASE || '.DERV_PRTF_INT_PSST') A
    USING IDENTIFIER(VTECH_DATABASE || '.DERV_PRTF_INT_HIST_PSST') B
    WHERE A.INT_GRUP_I = B.INT_GRUP_I
      AND PERIOD_OVERLAPS_UDF(ARRAY_CONSTRUCT(
            A.JOIN_FROM_D || '*' || A.JOIN_TO_D,
            B.JOIN_FROM_D || '*' || B.JOIN_TO_D
          )) = TRUE;
    
    rows_deleted := SQLROWCOUNT;
    
    -- STEP 5: Insert all deduped records into base
    INSERT INTO IDENTIFIER(STAR_DATABASE || '.DERV_PRTF_INT_PSST')
    SELECT
        A.INT_GRUP_I,                    
        A.INT_GRUP_TYPE_C,               
        A.JOIN_FROM_D,                   
        A.JOIN_TO_D,
        A.EFFT_D,
        A.EXPY_D,                 
        A.PTCL_N,                        
        A.REL_MNGE_I,                    
        A.VALD_FROM_D,                   
        A.VALD_TO_D,          
        0 AS PROS_KEY_I         
    FROM IDENTIFIER(VTECH_DATABASE || '.DERV_PRTF_INT_HIST_PSST') A;
    
    rows_inserted := SQLROWCOUNT;
    
    -- Snowflake equivalent of COLLECT STATISTICS (automatic optimization)
    -- In Snowflake, statistics are automatically maintained
    -- Optional: Force clustering if needed
    -- ALTER TABLE IDENTIFIER(STAR_DATABASE || '.DERV_PRTF_INT_PSST') RESUME RECLUSTER;
    
    result_msg := 'Successfully processed PRTF_TECH_INT_PSST: ' || 
                  'Deleted ' || rows_deleted || ' overlapping records, ' ||
                  'Inserted ' || rows_inserted || ' deduplicated records';
    
    RETURN result_msg;
    
EXCEPTION
    WHEN OTHER THEN
        RETURN 'Error in PRTF_TECH_INT_PSST_PROC: ' || SQLERRM;
END;
$$;

-- Alternative implementation without OVERLAPS UDF (using date range logic)
CREATE OR REPLACE PROCEDURE PRTF_TECH_INT_PSST_SIMPLE(
    STAR_DATABASE STRING DEFAULT 'STARDATADB',
    VTECH_DATABASE STRING DEFAULT 'VTECH'
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_deleted INTEGER := 0;
    rows_inserted INTEGER := 0;
BEGIN
    -- Delete overlapping records using date range logic
    DELETE FROM IDENTIFIER(STAR_DATABASE || '.DERV_PRTF_INT_PSST') A
    USING IDENTIFIER(VTECH_DATABASE || '.DERV_PRTF_INT_HIST_PSST') B
    WHERE A.INT_GRUP_I = B.INT_GRUP_I
      AND NOT (A.JOIN_FROM_D >= B.JOIN_TO_D OR B.JOIN_FROM_D >= A.JOIN_TO_D);
    
    rows_deleted := SQLROWCOUNT;
    
    -- Insert deduplicated records
    INSERT INTO IDENTIFIER(STAR_DATABASE || '.DERV_PRTF_INT_PSST')
    SELECT INT_GRUP_I, INT_GRUP_TYPE_C, JOIN_FROM_D, JOIN_TO_D, EFFT_D, EXPY_D,                 
           PTCL_N, REL_MNGE_I, VALD_FROM_D, VALD_TO_D, 0 AS PROS_KEY_I         
    FROM IDENTIFIER(VTECH_DATABASE || '.DERV_PRTF_INT_HIST_PSST');
    
    rows_inserted := SQLROWCOUNT;
    
    RETURN 'Processed: Deleted ' || rows_deleted || ', Inserted ' || rows_inserted;
END;
$$;

-- Required UDF for OVERLAPS functionality (create this first)
-- This should already exist from the UDF Helpers conversion

-- Usage examples:
-- CALL PRTF_TECH_INT_PSST_PROC('STARDATADB', 'VTECH');
-- CALL PRTF_TECH_INT_PSST_SIMPLE('STARDATADB', 'VTECH'); 