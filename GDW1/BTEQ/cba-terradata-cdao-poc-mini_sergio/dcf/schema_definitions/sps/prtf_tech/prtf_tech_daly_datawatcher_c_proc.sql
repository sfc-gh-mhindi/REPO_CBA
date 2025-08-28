CREATE OR REPLACE PROCEDURE PS_GDW1_BTEQ.BTEQ_SPS.PRTF_TECH_DALY_DATAWATCHER_C_PROC(
  ERROR_TABLE STRING DEFAULT 'PROCESS_ERROR_LOG',
  PROCESS_KEY STRING DEFAULT 'UNKNOWN_PROCESS'
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  error_code INTEGER DEFAULT 0;
  row_count INTEGER DEFAULT 0;
  source_found INTEGER DEFAULT 0;
BEGIN
  -- Check pre-requisite table loads have completed (ODS and Analytics)
  -- Ver 1.0 14/06/2013 T Jelliffe - Initial Version
  
  -- Check Analytics at table level and verify all dependencies are loaded
  SELECT COUNT(*) INTO :source_found
  FROM (
    SELECT SRCE_FND
    FROM (
      -- Check Analytics at table level
      SELECT 1 AS SRCE_FND
      FROM (
        SELECT 
          UPI.TRGT_M AS TRGT_M,
          UPI.SRCE_M as SRCE_M
        FROM ps_gdw1_bteq.PVTECH.UTIL_PROS_ISAC AS UPI
        -- Want the data loaded for yesterday
        WHERE UPI.BTCH_RUN_D = CURRENT_DATE - 1
        AND UPI.TRGT_M IN (
          -- Get the list of pre-requisite tables 
          SELECT UP1.PARM_LTRL_STRG_X
          FROM ps_gdw1_bteq.PVTECH.UTIL_PARM UP1
          WHERE UP1.PARM_M = 'PRTF_TECH_CHNG_SCHE_DEPN_TABL'
        )
        -- Get the list of pre-requisite sources
        AND UPI.SRCE_M LIKE ANY (
          SELECT '%'||TRIM(UP1.PARM_LTRL_STRG_X)||'%' AS SRCE_M
          FROM ps_gdw1_bteq.PVTECH.UTIL_PARM UP1
          WHERE UP1.PARM_M = 'PRTF_TECH_CHNG_SCHE_DEPN_SRCE'
        )
        -- and check they have been loaded
        AND UPI.COMT_F = 'Y'
        QUALIFY RANK() OVER (PARTITION BY UPI.TRGT_M ORDER BY UPI.BTCH_RUN_D DESC) = 1
      ) DT (TRGT_M, SRCE_M)
      -- and check all relevant sources have loaded
      HAVING COUNT(TRGT_M) = (
        SELECT UP2.PARM_LTRL_N
        FROM ps_gdw1_bteq.PVTECH.UTIL_PARM UP2
        WHERE UP2.PARM_M = 'PRTF_TECH_CHNG_SCHE_DEPN_SRCE_LOAD'
      )
    ) SRCES
    GROUP BY 1
    HAVING COUNT(SRCE_FND) = 1
  );
  
  -- Handle the three exit conditions from original BTEQ
  IF (:source_found = 0) THEN
    -- Equivalent to REPOLL label - dependencies not ready
    RETURN 'REPOLL: Dependencies not ready for processing';
  ELSEIF (:source_found = 1) THEN
    -- Equivalent to successful exit - all dependencies loaded
    RETURN 'SUCCESS: All prerequisite table loads completed';
  ELSE
    -- Unexpected condition
    RETURN 'ERROR: Unexpected dependency check result: ' || :source_found;
  END IF;
  
EXCEPTION
  WHEN OTHER THEN
    -- Equivalent to EXITERR label
    RETURN 'ERROR: ' || SQLERRM || ' (Code: ' || SQLCODE || ')';
END;
$$;