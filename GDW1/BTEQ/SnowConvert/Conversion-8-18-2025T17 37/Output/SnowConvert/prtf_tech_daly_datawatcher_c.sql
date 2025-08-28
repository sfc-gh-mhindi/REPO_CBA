!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.RUN FILE=%%BTEQ_LOGON_SCRIPT%%

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.SET QUIET OFF

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.SET ECHOREQ ON

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.SET FORMAT OFF

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
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
SELECT
 SRCE_FND
FROM
(
  --Check Analytics at table level
  SELECT
   1 AS SRCE_FND
   FROM
   (
    SELECT
   --AND UPI.SRCE_SYST_M = 'SAP'
     UPI.TRGT_M AS TRGT_M AS TRGT_M,
     UPI.SRCE_M as SRCE_M AS SRCE_M
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '34' COLUMN '8' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '34' COLUMN '8'. FAILED TOKEN WAS '%' ON LINE '34' COLUMN '13'. **
--       FROM %%VTECH%%.UTIL_PROS_ISAC AS UPI

    -- Want the data loaded for yesterday

           WHERE UPI.BTCH_RUN_D = CURRENT_DATE() - 1
           AND UPI.TRGT_M IN
                             --** SSC-FDM-0002 - CORRELATED SUBQUERIES MAY HAVE SOME FUNCTIONAL DIFFERENCES. **
                             (
                              --Get the list of pre-requisite tables 
                              SELECT
                               UP1.PARM_LTRL_STRG_X
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '42' COLUMN '29' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '42' COLUMN '29'. FAILED TOKEN WAS '%' ON LINE '42' COLUMN '34'. **
--                            FROM %%VTECH%%.UTIL_PARM UP1
                                                          WHERE
                               UPPER(RTRIM( UP1.PARM_M)) = UPPER(RTRIM('PRTF_TECH_CHNG_SCHE_DEPN_TABL'))
                               )
    --Get the list of pre-requisite sources
           AND UPI.SRCE_M LIKE ANY
                    (
                              SELECT
                               '%'||TRIM(UP1.PARM_LTRL_STRG_X)||'%' AS SRCE_M
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '49' COLUMN '12' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '49' COLUMN '12'. FAILED TOKEN WAS '%' ON LINE '49' COLUMN '17'. **
--           FROM %%VTECH%%.UTIL_PARM UP1
                                        WHERE
                               UPPER(RTRIM( UP1.PARM_M)) = UPPER(RTRIM('PRTF_TECH_CHNG_SCHE_DEPN_SRCE'))
                    )
    --and check they have been loaded
           AND UPPER(RTRIM( UPI.COMT_F)) = UPPER(RTRIM('Y'))
    QUALIFY
           RANK() OVER (PARTITION BY UPI.TRGT_M
                                    ORDER BY UPI.BTCH_RUN_D DESC NULLS LAST) = 1
   ) DT

  --and check all relevant sources have loaded
   HAVING
   COUNT(TRGT_M) =
   --** SSC-FDM-0002 - CORRELATED SUBQUERIES MAY HAVE SOME FUNCTIONAL DIFFERENCES. **
   (
    SELECT
           ANY_VALUE(UP2.PARM_LTRL_N)
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '64' COLUMN '10' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '64' COLUMN '10'. FAILED TOKEN WAS '%' ON LINE '64' COLUMN '15'. **
--         FROM %%VTECH%%.UTIL_PARM UP2
    WHERE
           UPPER(RTRIM( UP2.PARM_M)) = UPPER(RTRIM('PRTF_TECH_CHNG_SCHE_DEPN_SRCE_LOAD'))
   )

) SRCES
GROUP BY 1
HAVING
 COUNT(SRCE_FND) = 1;


-- This info is for CBM use only
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-04 09:44:07 +1000 (Thu, 04 Jul 2013) $
-- $LastChangedRevision: 12230 $
!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.IF ERRORCODE <> 0    THEN .GOTO EXITERR

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ACTIVITYCOUNT = 0    THEN .GOTO REPOLL

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.QUIT 0

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.LOGOFF

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.LABEL EXITERR

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.QUIT 1

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.LOGOFF

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.LABEL REPOLL

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.QUIT 2

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.LOGOFF

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.EXIT