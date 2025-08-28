-- =====================================================================
-- DBT Model: DERV_ACCT_PATY_00_DATAWATCHER
-- Converted from BTEQ: DERV_ACCT_PATY_00_DATAWATCHER.sql
-- Category: derived_account_party
-- Original Size: 5.3KB, 173 lines
-- Complexity Score: 67
-- Generated: 2025-08-21 15:50:26
-- =====================================================================

{{ intermediate_model_config() }}




------------------------------------------------------------------------------
--  SCRIPT NAME: DERV_ACCT_PATY_00_DATAWATCHER.sql
--
--  DESCRIPTION: This script is a datawatcher for the load of STAR_CAD_PROD_DATA.DERV_ACCT_PATY.
--               This load depends on the completion of load of PDPATY.ACCT_PATY and persisted portfolio tables
---              for the required date.  Check PVPATY.UTIL_PROS_ISAC for  the completion of a load of number
--               of tables stored in  STAR_CAD_PROD_DATA.UTIL_PARM, as well as  PVTECH.UTIL_PROS_ISAC for 
--               the completion of the load of portfolio persisted tables.
--               This code is inspired by Megan Disch's datawatcher code
--               written for CMPF and modified for the needs of this extract.
--
--  MODIFICATION HISTORY:
--
--  VER  DATE       MODIFIED BY            DESCRIPTION
--  ---- ---------- ---------------------- -----------------------------------
--  1.0  21/06/2013 Helen Zak              Initial version
--  1.1  14/08/2013 Helen Zak              C0714577 - post-implementation fix
--                                         Look for BTCH_RUN_D >= :EXTR_D to cater
--                                         for missed runs or PRTF_TECH as this is
--                                         the run that is refreshed every time it runs
--                                         so as long as it ran for any date AFTER our extract
--                                         date, we will get the correct data for the
--                                         required date. 
----------------------------------------------------------------------------
-- This info is for CBM use only
-- $LastChangedBy: zakhe $
-- $LastChangedDate: 2013-08-15 13:36:13 +1000 (Thu, 15 Aug 2013) $
-- $LastChangedRevision: 12450 $
--   

-- Remove the existing file containing extract date


-- 1. Calculate the the next extract date.  To do that, get the latest date for which our load has run  and
-- add 1 day to it to determine whether the loads of interest have completed for that date. 
--This will be the date for which the datawatcher will be expecting the data to be loaded



   SELECT CAST(BTCH_RUN_D + 1 AS CHAR(10))
     FROM {{ bteq_var("VTECH") }}.UTIL_BTCH_ISAC UBI

      WHERE UBI.BTCH_STUS_C = 'COMT'
        AND UBI.SRCE_SYST_M = '{{ bteq_var("SRCE_SYST_M") }}'                    
       
    QUALIFY RANK() OVER (ORDER BY BTCH_RUN_D  DESC,STUS_CHNG_S DESC) = 1
  ;
  




-- 2. Check if all required loads completed for the required  date
--    or for the subsequent dates for PRTF_TECH



USING 
( EXTR_D VARCHAR(10) )
SELECT SRCE_FND
FROM
(
-- Check SAP BP loads at table level

 SELECT 1 AS SRCE_FND
 FROM (
       SELECT UPI.TRGT_M AS TRGT_M
             ,UPI.SRCE_SYST_M AS SRCE_M
         FROM  {{ bteq_var("VPATY") }}.UTIL_PROS_ISAC UPI
        WHERE BTCH_RUN_D IS NOT NULL
          AND BTCH_RUN_D <> '0001-01-01'

-- Want the data loaded for the current extract date
       
          AND UPI.BTCH_RUN_D  = :EXTR_D
          AND UPI.TRGT_M IN (
      
--Get the list of pre-requisite tables
 
                            SELECT UP1.PARM_LTRL_STRG_X
                            FROM {{ bteq_var("VTECH") }}.UTIL_PARM UP1
                            WHERE UP1.PARM_M = 'DERV_ACCT_PATY_SAPBP_SCHE_DEPN_TABL'
                           )
                           
--Get the list of pre-requisite sources

         AND UPI.SRCE_SYST_M IN
          (
           SELECT UP1.PARM_LTRL_STRG_X AS SRCE_SYST_M
           FROM {{ bteq_var("VTECH") }}.UTIL_PARM UP1
           WHERE UP1.PARM_M = 'DERV_ACCT_PATY_SAPBP_SCHE_DEPN_SRCE'
          )
          
--and check they have been loaded

      AND UPI.COMT_F = 'Y'
     

       QUALIFY RANK() OVER (PARTITION BY UPI.TRGT_M
                                ORDER BY UPI.BTCH_RUN_D DESC) = 1
      ) DT (TRGT_M, SRCE_M)

--and check all relevant sources have loaded

 HAVING COUNT(TRGT_M) =
        (
         SELECT UP2.PARM_LTRL_N
         FROM {{ bteq_var("VTECH") }}.UTIL_PARM UP2
         WHERE UP2.PARM_M = 'DERV_ACCT_PATY_SAPBP_SCHE_DEPN_SRCE_LOAD'
        )
 
 UNION ALL

-- Check portfolio persisted tables loads at stream level

 SELECT 1 AS SRCE_FND
 FROM (
       SELECT UBI.SRCE_SYST_M 
       FROM {{ bteq_var("VTECH") }}.UTIL_BTCH_ISAC UBI

      WHERE UBI.BTCH_STUS_C = 'COMT'
        AND UBI.SRCE_SYST_M = 'PRTF_TECH'                    
        AND UBI.BTCH_RUN_D  >= :EXTR_D
    QUALIFY RANK() OVER (ORDER BY BTCH_RUN_D  DESC,STUS_CHNG_S DESC) = 1
    
     ) DT2 (SRCE_SYST_M)
 HAVING COUNT(SRCE_SYST_M) = 1
 
) SRCES
GROUP BY 1
HAVING COUNT(SRCE_FND) = (
         SELECT UP2.PARM_LTRL_N
         FROM {{ bteq_var("VTECH") }}.UTIL_PARM UP2
         WHERE UP2.PARM_M = 'DERV_ACCT_PATY_SCHE_DEPN_SRCE_LOAD'
        )    
      
  
;






