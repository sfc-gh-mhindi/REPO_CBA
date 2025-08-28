!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.run file=%%BTEQ_LOGON_SCRIPT%%

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORLEVEL <> 0 THEN .GOTO EXITERR

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.SET QUIET OFF

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.SET ECHOREQ ON

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.SET FORMAT OFF

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.SET WIDTH 120

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.SET RECORDMODE OFF

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.SET FOLDLINE ON ALL

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
!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.OS rm /cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt

-- 1. Calculate the the next extract date.  To do that, get the latest date for which our load has run  and
-- add 1 day to it to determine whether the loads of interest have completed for that date. 
--This will be the date for which the datawatcher will be expecting the data to be loaded
!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!


.EXPORT DATA FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.SET RECORDMODE OFF

SELECT
 RPAD(CAST(BTCH_RUN_D + 1 AS CHAR(10)), 10)
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '55' COLUMN '6' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '55' COLUMN '6'. FAILED TOKEN WAS '%' ON LINE '55' COLUMN '11'. **
--     FROM %%VTECH%%.UTIL_BTCH_ISAC UBI
      WHERE
 UPPER(RTRIM( UBI.BTCH_STUS_C)) = UPPER(RTRIM('COMT'))
        AND UPPER(RTRIM( UBI.SRCE_SYST_M)) = UPPER(RTRIM('%%SRCE_SYST_M%%'))
QUALIFY
 RANK() OVER (ORDER BY BTCH_RUN_D DESC NULLS LAST,STUS_CHNG_S DESC NULLS LAST) = 1;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.EXPORT RESET

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!


.IF ERRORCODE <> 0 THEN .GOTO EXITERR

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ACTIVITYCOUNT <> 1 THEN .GOTO EXITERR
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '75' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '75' COLUMN '2'. **
--                                         IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
USING
( EXTR_D VARCHAR(10) )
SELECT
 SRCE_FND
FROM
(
  -- Check SAP BP loads at table level
  SELECT
   1 AS SRCE_FND
   FROM
   (
    SELECT
     UPI.TRGT_M AS TRGT_M AS TRGT_M,
     UPI.SRCE_SYST_M AS SRCE_M AS SRCE_M
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '87' COLUMN '10' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '87' COLUMN '10'. FAILED TOKEN WAS '%' ON LINE '87' COLUMN '16'. **
--         FROM  %%VPATY%%.UTIL_PROS_ISAC UPI
            WHERE BTCH_RUN_D IS NOT NULL
              AND UPPER(RTRIM( BTCH_RUN_D)) <> UPPER(RTRIM('0001-01-01'))

    -- Want the data loaded for the current extract date

              AND UPI.BTCH_RUN_D = :EXTR_D
              AND UPI.TRGT_M IN
                                --** SSC-FDM-0002 - CORRELATED SUBQUERIES MAY HAVE SOME FUNCTIONAL DIFFERENCES. **
                                (
                                 --Get the list of pre-requisite tables
                                 SELECT
                                  UP1.PARM_LTRL_STRG_X
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '99' COLUMN '29' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '99' COLUMN '29'. FAILED TOKEN WAS '%' ON LINE '99' COLUMN '34'. **
--                            FROM %%VTECH%%.UTIL_PARM UP1
                                                             WHERE
                                  UPPER(RTRIM( UP1.PARM_M)) = UPPER(RTRIM('DERV_ACCT_PATY_SAPBP_SCHE_DEPN_TABL'))
                               )

    --Get the list of pre-requisite sources

             AND UPI.SRCE_SYST_M IN
              (
                                 SELECT
                                  UP1.PARM_LTRL_STRG_X AS SRCE_SYST_M
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '108' COLUMN '12' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '108' COLUMN '12'. FAILED TOKEN WAS '%' ON LINE '108' COLUMN '17'. **
--           FROM %%VTECH%%.UTIL_PARM UP1
                                           WHERE
                                  UPPER(RTRIM( UP1.PARM_M)) = UPPER(RTRIM('DERV_ACCT_PATY_SAPBP_SCHE_DEPN_SRCE'))
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
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '126' COLUMN '10' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '126' COLUMN '10'. FAILED TOKEN WAS '%' ON LINE '126' COLUMN '15'. **
--         FROM %%VTECH%%.UTIL_PARM UP2
    WHERE
          UPPER(RTRIM( UP2.PARM_M)) = UPPER(RTRIM('DERV_ACCT_PATY_SAPBP_SCHE_DEPN_SRCE_LOAD'))
   )
 UNION ALL
  -- Check portfolio persisted tables loads at stream level
  SELECT
   1 AS SRCE_FND
   FROM
   (
    SELECT
          UBI.SRCE_SYST_M AS SRCE_SYST_M
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '137' COLUMN '8' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '137' COLUMN '8'. FAILED TOKEN WAS '%' ON LINE '137' COLUMN '13'. **
--       FROM %%VTECH%%.UTIL_BTCH_ISAC UBI

         WHERE
          UPPER(RTRIM( UBI.BTCH_STUS_C)) = UPPER(RTRIM('COMT'))
           AND UPPER(RTRIM( UBI.SRCE_SYST_M)) = UPPER(RTRIM('PRTF_TECH'))
           AND UBI.BTCH_RUN_D >= :EXTR_D
    QUALIFY
          RANK() OVER (ORDER BY BTCH_RUN_D DESC NULLS LAST,STUS_CHNG_S DESC NULLS LAST) = 1
   ) DT2
   HAVING
   COUNT(SRCE_SYST_M) = 1

) SRCES
GROUP BY 1
HAVING
 COUNT(SRCE_FND) =
                   --** SSC-FDM-0002 - CORRELATED SUBQUERIES MAY HAVE SOME FUNCTIONAL DIFFERENCES. **
                   (
  SELECT
   ANY_VALUE(UP2.PARM_LTRL_N)
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '151' COLUMN '10' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '151' COLUMN '10'. FAILED TOKEN WAS '%' ON LINE '151' COLUMN '15'. **
--         FROM %%VTECH%%.UTIL_PARM UP2
          WHERE
   UPPER(RTRIM( UP2.PARM_M)) = UPPER(RTRIM('DERV_ACCT_PATY_SCHE_DEPN_SRCE_LOAD'))
         )
--;

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '158' COLUMN '1' OF THE SOURCE CODE STARTING AT '.'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS ';' ON LINE '156' COLUMN '1'. FAILED TOKEN WAS '.' ON LINE '158' COLUMN '1'. **
--.
 ;
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '158' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IF'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '75' COLUMN '2'. FAILED TOKEN WAS 'IF' ON LINE '158' COLUMN '2'. **
--  IF ERRORCODE <> 0 THEN .GOTO EXITERR

--.IF ACTIVITYCOUNT = 0 THEN .GOTO REPOLL

--.QUIT 0
--.LOGOFF

--.LABEL EXITERR
--.QUIT 1
--.LOGOFF

--.LABEL REPOLL
--.QUIT 2
--.LOGOFF
--.EXIT