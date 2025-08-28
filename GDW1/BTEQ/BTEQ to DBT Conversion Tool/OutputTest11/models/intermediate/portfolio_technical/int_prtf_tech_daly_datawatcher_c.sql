-- =====================================================================
-- DBT Model: prtf_tech_daly_datawatcher_c
-- Converted from BTEQ: prtf_tech_daly_datawatcher_c.sql
-- Category: portfolio_technical
-- Original Size: 2.6KB, 94 lines
-- Complexity Score: 34
-- Generated: 2025-08-21 15:50:26
-- =====================================================================

{{ intermediate_model_config() }}



------------------------------------------------------------------------------
--
--
--  Ver  Date       Modified By            Description
--  ---- ---------- ---------------------- -----------------------------------
--  1.0  14/06/2013 T Jelliffe             Initial Version
------------------------------------------------------------------------------

-- Check pre-requisite table loads have completed (ODS and Analytics)

-- To add a new dependency, insert a PRTF_TECH_CHNG_SCHE_DEPN_TABL row and a 
-- PRTF_TECH_CHNG_SCHE_DEPN_SRCE row in UTIL_PARM and increment parm
-- CMPF_MCIB_SCHE_DEPN_DNCT_TABL by number of load processes required (eg. at
-- time of build, EVNT had 2 loads, one of which had more than one source)
------------------------------------------------------------------------------

SELECT SRCE_FND
FROM
(
--Check Analytics at table level
 SELECT 1 AS SRCE_FND
 FROM (
       SELECT 
         UPI.TRGT_M AS TRGT_M
        ,UPI.SRCE_M as SRCE_M
       FROM {{ bteq_var("VTECH") }}.UTIL_PROS_ISAC AS UPI

-- Want the data loaded for yesterday

       WHERE UPI.BTCH_RUN_D = CURRENT_DATE - 1
       AND UPI.TRGT_M IN (
--Get the list of pre-requisite tables 
                            SELECT UP1.PARM_LTRL_STRG_X
                            FROM {{ bteq_var("VTECH") }}.UTIL_PARM UP1
                            WHERE UP1.PARM_M = 'PRTF_TECH_CHNG_SCHE_DEPN_TABL'
                           )
--Get the list of pre-requisite sources
       AND UPI.SRCE_M LIKE ANY
          (
           SELECT '%'||TRIM(UP1.PARM_LTRL_STRG_X)||'%' AS SRCE_M
           FROM {{ bteq_var("VTECH") }}.UTIL_PARM UP1
           WHERE UP1.PARM_M = 'PRTF_TECH_CHNG_SCHE_DEPN_SRCE'
          )
--and check they have been loaded
       AND UPI.COMT_F = 'Y'
       --AND UPI.SRCE_SYST_M = 'SAP'

       QUALIFY RANK() OVER (PARTITION BY UPI.TRGT_M
                                ORDER BY UPI.BTCH_RUN_D DESC) = 1
      ) DT (TRGT_M, SRCE_M)

--and check all relevant sources have loaded
 HAVING COUNT(TRGT_M) =
        (
         SELECT UP2.PARM_LTRL_N
         FROM {{ bteq_var("VTECH") }}.UTIL_PARM UP2
         WHERE UP2.PARM_M = 'PRTF_TECH_CHNG_SCHE_DEPN_SRCE_LOAD'
        )
       
) SRCES
GROUP BY 1
HAVING COUNT(SRCE_FND) = 1    
;


-- This info is for CBM use only
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-04 09:44:07 +1000 (Thu, 04 Jul 2013) $
-- $LastChangedRevision: 12230 $

                                                 




