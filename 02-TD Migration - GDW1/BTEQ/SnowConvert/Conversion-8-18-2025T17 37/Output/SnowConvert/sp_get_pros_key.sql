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
--  1.0  11/06/2013 T Jelliffe             Initial Version
------------------------------------------------------------------------------
--
-- This info is for CBM use only
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-03 15:57:40 +1000 (Wed, 03 Jul 2013) $
-- $LastChangedRevision: 12229 $
--
!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.EXPORT RESET

-- Remove the existing DATE file
!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.OS rm /cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_%%TBSHORT%%PROS_KEY.txt
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '28' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '28' COLUMN '2'. **
--                                                                            IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_BTCH_KEY
                                                                                                                                                      
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '28' COLUMN '77' OF THE SOURCE CODE STARTING AT 'txt'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS '.' ON LINE '28' COLUMN '76'. FAILED TOKEN WAS 'txt' ON LINE '28' COLUMN '77'. **
--                                                                                                                                                      txt

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.EXPORT DATA FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_%%TBSHORT%%PROS_KEY.txt

USING
(
  FILLER CHAR(24),
  BTCHKEY CHAR(10) )
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '36' COLUMN '1' OF THE SOURCE CODE STARTING AT 'CALL'. EXPECTED 'Call' GRAMMAR. LAST MATCHING TOKEN WAS 'CALL' ON LINE '36' COLUMN '1'. FAILED TOKEN WAS '%' ON LINE '36' COLUMN '6'. **
--CALL %%STARMACRDB%%.SP_GET_PROS_KEY(
--  '%%GDW_USER%%',           -- From the config.pass parameters
--  '%%SRCE_SYST_M%%',
--  '%%SRCE_M%%',
--  '%%PSST_TABLE_M%%',
--  CAST(trim(:BTCHKEY) as DECIMAL(10,0)),
--  CAST(%%INDATE%% as DATE FORMAT'YYYYMMDD'),
--  '%%RSTR_F%%',                -- Restart Flag
--  'MVS',
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