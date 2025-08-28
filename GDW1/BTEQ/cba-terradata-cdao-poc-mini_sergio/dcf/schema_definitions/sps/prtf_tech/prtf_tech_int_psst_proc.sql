CREATE OR REPLACE PROCEDURE PS_GDW1_BTEQ.BTEQ_SPS.PRTF_TECH_INT_PSST_PROC()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  error_code INTEGER DEFAULT 0;
  delete_count INTEGER DEFAULT 0;
  insert_count INTEGER DEFAULT 0;
BEGIN
  ------------------------------------------------------------------------------
  --
  --  Ver  Date       Modified By        Description
  --  ---- ---------- ------------------ ---------------------------------------
  --  1.0  11/06/2013 T Jelliffe         Initial Version
  --  1.1  11/07/2013 T Jelliffe         Use PROS_KEY_EFFT_I prevent self join
  --  1.2  12/07/2013 T Jelliffe         Time period reduced 15 to 3 years
  --  1.3  27/08/2013 T Jelliffe         Use only records with same EFFT_D
  --  1.4  21/10/2013 T Jelliffe         Insert/Delete changed records
  --  1.5  01/11/2013 T Jelliffe         Bug fixes - Row_Number not Rank
  ------------------------------------------------------------------------------
  
  --<================================================>--
  --< STEP 4 Delete all the original overlap records >--
  --<================================================>--
  
  DELETE FROM PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_INT_PSST
  WHERE EXISTS (
    SELECT 1
    FROM ps_gdw1_bteq.PVTECH.DERV_PRTF_INT_HIST_PSST B
    WHERE 
      DERV_PRTF_INT_PSST.INT_GRUP_I = B.INT_GRUP_I
      AND DERV_PRTF_INT_PSST.JOIN_FROM_D <= B.JOIN_TO_D AND DERV_PRTF_INT_PSST.JOIN_TO_D >= B.JOIN_FROM_D
  );
  
  delete_count := SQLROWCOUNT;
  
  --<===============================================>--
  --< STEP 5 - Insert all deduped records into base >--
  --<===============================================>--
  
  INSERT INTO PS_CLD_RW.STARCADPRODDATA.DERV_PRTF_INT_PSST
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
     0 as PROS_KEY_I
  FROM 
    ps_gdw1_bteq.PVTECH.DERV_PRTF_INT_HIST_PSST A;
  
  insert_count := SQLROWCOUNT;
  
  -- Collect Statistics equivalent (Snowflake auto-manages statistics)
  -- Statistics collection is automatic in Snowflake
  
  RETURN 'SUCCESS: Deleted ' || :delete_count || ' overlapping records, inserted ' || :insert_count || ' deduped records';
  
EXCEPTION
  WHEN OTHER THEN
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;