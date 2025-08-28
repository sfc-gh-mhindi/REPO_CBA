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
-- Object Name             :  DERV_ACCT_PATY_01_SP_GET_PROS_KEY.sql
-- Object Type             :  BTEQ

--                           
-- Description             :  Get process key for the batch key and the date  
--     
--
--  Ver  Date       Modified By            Description
--  ---- ---------- ---------------------- -----------------------------------
--  1.0  24/07/2013 Helen Zak              Initial Version
------------------------------------------------------------------------------
--
-- This info is for CBM use only
-- $LastChangedBy: zakhe $
-- $LastChangedDate: 2013-07-25 12:29:28 +1000 (Thu, 25 Jul 2013) $
-- $LastChangedRevision: 12363 $
--



-- Remove the existing pros key  and btch date files
!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.OS rm /cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_PROS_KEY.txt

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.OS rm /cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_BTCH_DATE.txt
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '36' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '36' COLUMN '2'. **
--                                                                  IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_BTCH_KEY
                                                                                                                                            
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '36' COLUMN '77' OF THE SOURCE CODE STARTING AT 'txt'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS '.' ON LINE '36' COLUMN '76'. FAILED TOKEN WAS 'txt' ON LINE '36' COLUMN '77'. **
--                                                                                                                                            txt

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.EXPORT DATA FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_BTCH_DATE.txt
USING
(
  FILLER CHAR(24),
  BTCHKEY CHAR(10))
SELECT
  RPAD(TO_VARCHAR(CAST(TRUNC(BTCH_KEY_I) AS INTEGER) !!!RESOLVE EWI!!! /*** SSC-EWI-TD0041 - TRUNC FUNCTION WAS ADDED TO ENSURE INTEGER. MAY NEED CHANGES IF NOT NUMERIC OR STRING. ***/!!!, 'TM'), 10),
  RPAD(CAST(BTCH_RUN_D AS CHAR(10)), 10)

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '45' COLUMN '5' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '45' COLUMN '5'. FAILED TOKEN WAS '%' ON LINE '45' COLUMN '10'. **
--    FROM %%VTECH%%.UTIL_BTCH_ISAC
  WHERE BTCH_KEY_I = :BTCHKEY;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!


.EXPORT RESET

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

 .IF ERRORCODE <> 0    THEN .GOTO EXITERR
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '56' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '56' COLUMN '2'. **
--                                         IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_BTCH_DATE
                                                                                                                    
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '56' COLUMN '78' OF THE SOURCE CODE STARTING AT 'txt'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS '.' ON LINE '56' COLUMN '77'. FAILED TOKEN WAS 'txt' ON LINE '56' COLUMN '78'. **
--                                                                                                                    txt

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.EXPORT DATA FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_PROS_KEY.txt

USING

(  FILLER CHAR(4)
   ,BTCHKEY CHAR(10)
   ,EXTR_D CHAR(10)
   )
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '65' COLUMN '1' OF THE SOURCE CODE STARTING AT 'CALL'. EXPECTED 'Call' GRAMMAR. LAST MATCHING TOKEN WAS 'CALL' ON LINE '65' COLUMN '1'. FAILED TOKEN WAS '%' ON LINE '65' COLUMN '6'. **
--CALL %%STARMACRDB%%.SP_GET_PROS_KEY(
--  '%%GDW_USER%%',           -- From the config.pass parameters
--  '%%SRCE_SYST_M%%',
--  '%%SRCE_M%%',
--  '%%PSST_TABLE_M%%',
--  CAST(TRIM(:BTCHKEY) as DECIMAL(10,0)),
--  CAST(:EXTR_D AS DATE) (FORMAT'YYYYMMDD')(CHAR(8)),
--  '%%RSTR_F%%',                -- Restart Flag
--  'TD',
--  IPROCESSKEY (CHAR(100))
--)
 ;

  !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.EXPORT RESET

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.IF ERRORCODE <> 0    THEN .GOTO EXITERR

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
.EXIT