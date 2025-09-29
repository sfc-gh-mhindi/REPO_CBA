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
-- Object Name             :  DERV_ACCT_PATY_01_SP_GET_BTCH_KEY.sql
-- Object Type             :  BTEQ

--                           
-- Description             :  Call a stored procedure to obtain batch key for the extract date  
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
!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.EXPORT RESET

-- Remove the existing batch key file
!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.OS rm /cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_BTCH_KEY.txt

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.EXPORT DATA FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_BTCH_KEY.txt

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.SET FORMAT OFF
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '40' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '40' COLUMN '2'. **
--               IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
--USING
--( EXTR_D VARCHAR(10) )
--CALL %%STARMACRDB%%.SP_GET_BTCH_KEY(
--  '%%SRCE_SYST_M%%'
--  ,CAST(:EXTR_D AS DATE) (FORMAT'YYYYMMDD')(CHAR(8))
--  ,IBATCHKEY (CHAR(80))
--)
 ;

/* NOTE: BTCH_KEY is returned as a string in the format where     */
/* First 24 characters ignored then next 10 are the actual number */!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

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