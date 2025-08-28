.RUN FILE=${ETL_APP_BTEQ}/bteq_login.sql
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

.SET QUIET OFF
.SET ECHOREQ ON
.SET FORMAT OFF
.SET WIDTH 120

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
        ,UPI.DERIVED_ACCOUNT_PARTY as DERIVED_ACCOUNT_PARTY
       FROM PVTECH.UTIL_PROS_ISAC AS UPI

-- Want the data loaded for yesterday

       WHERE UPI.BTCH_RUN_D = CURRENT_DATE - 1
       AND UPI.TRGT_M IN (
--Get the list of pre-requisite tables 
                            SELECT UP1.PARM_LTRL_STRG_X
                            FROM PVTECH.UTIL_PARM UP1
                            WHERE UP1.PARM_M = 'PRTF_TECH_CHNG_SCHE_DEPN_TABL'
                           )
--Get the list of pre-requisite sources
       AND UPI.DERIVED_ACCOUNT_PARTY LIKE ANY
          (
           SELECT '%'||TRIM(UP1.PARM_LTRL_STRG_X)||'%' AS DERIVED_ACCOUNT_PARTY
           FROM PVTECH.UTIL_PARM UP1
           WHERE UP1.PARM_M = 'PRTF_TECH_CHNG_SCHE_DEPN_SRCE'
          )
--and check they have been loaded
       AND UPI.COMT_F = 'Y'
       --AND UPI.DERIVED_ACCOUNT_PARTY = 'SAP'

       QUALIFY RANK() OVER (PARTITION BY UPI.TRGT_M
                                ORDER BY UPI.BTCH_RUN_D DESC) = 1
      ) DT (TRGT_M, DERIVED_ACCOUNT_PARTY)

--and check all relevant sources have loaded
 HAVING COUNT(TRGT_M) =
        (
         SELECT UP2.PARM_LTRL_N
         FROM PVTECH.UTIL_PARM UP2
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

.IF ERRORCODE <> 0    THEN .GOTO EXITERR
.IF ACTIVITYCOUNT = 0    THEN .GOTO REPOLL
                                                 
.QUIT 0
.LOGOFF

.LABEL EXITERR
.QUIT 1
.LOGOFF

.LABEL REPOLL
.QUIT 2
.LOGOFF
.EXIT


