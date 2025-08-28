!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.RUN FILE=%%BTEQ_LOGON_SCRIPT%%

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

--------------------------------------------------------------------------------
---- Object Name             :  DERV_ACCT_PATY_03_SET_ACCT_PRTF.sql
---- Object Type             :  BTEQ

----                           
---- Description             :  Get accounts that are relationship managed and ONLY ONE
----                            of the parties oin this account is relationship managed 
----                            by the same RM. This party will be a preferred party for
----                            such an account.   
----                          
--------------------------------------------------------------------------------
---- Modification History
---- Date               Author           Version     Version Description
---- 04/06/2013         Helen Zak        1.0         Initial Version
----07/08/2013          Helen Zak        1.1         C0714578 - post-implementation fix
----                                                 Use persisted GRD table for better performance 
----                                                 Only get existing rows that have the flag set to 'N'
----                                                 as otherwise they don't need to change 
-- -- 21/08/2013       Helen Zak       1.2          C0726912  - post-implementation fix
-- --                                                                         Remove logic of getting existing rows
-- --                                                                         as it should be handled by the delta process (in theory)
--------------------------------------------------------------------------------

---- Get account portfolio details as per the extract date
--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 36 OF FILE: /Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/DERV_ACCT_PATY_03_SET_ACCT_PRTF.sql ***/!!!


--DELETE
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '36' COLUMN '7' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '36' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '36' COLUMN '12'. **
--       FROM %%DDSTG%%.DERV_PRTF_ACCT_STAG
                                         ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '38' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '38' COLUMN '2'. **
--                                       IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
USING
( EXTR_D VARCHAR(10) )
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '41' COLUMN '1' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '41' COLUMN '8'. **
--INSERT INTO %%DDSTG%%.DERV_PRTF_ACCT_STAG
-- SELECT ACCT_I
--        ,PRTF_CODE_X
--   FROM  %%VTECH%%.DERV_PRTF_ACCT

-- WHERE PERD_D = :EXTR_D
--   AND DERV_PRTF_CATG_C = 'RM'

-- GROUP BY 1,2
             ;

 !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.IF ERRORCODE   <> 0 THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '53' COLUMN '1' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '53' COLUMN '1'. **
--COLLECT STATS %%DDSTG%%.DERV_PRTF_ACCT_STAG
                                           ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR

---- Get party portfolio details as per the extract date
--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 58 OF FILE: /Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/DERV_ACCT_PATY_03_SET_ACCT_PRTF.sql ***/!!!

--DELETE
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '58' COLUMN '7' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '58' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '58' COLUMN '12'. **
--       FROM %%DDSTG%%.DERV_PRTF_PATY_STAG
                                         ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '61' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '61' COLUMN '2'. **
--                                       IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
USING
( EXTR_D VARCHAR(10) )
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '64' COLUMN '1' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '64' COLUMN '8'. **
--INSERT INTO %%DDSTG%%.DERV_PRTF_PATY_STAG
-- SELECT PATY_I
--        ,PRTF_CODE_X
--   FROM  %%VTECH%%.DERV_PRTF_PATY

-- WHERE PERD_D = :EXTR_D
--   AND DERV_PRTF_CATG_C = 'RM'

-- GROUP BY 1,2
             ;

 !!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '75' COLUMN '1' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '75' COLUMN '1'. **
--COLLECT STATS %%DDSTG%%.DERV_PRTF_PATY_STAG
                                           ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!



.QUIT 0

-- $LastChangedBy: zakhe $
-- $LastChangedDate: 2013-08-22 12:10:19 +1000 (Thu, 22 Aug 2013) $
-- $LastChangedRevision: 12529 $
!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.LABEL EXITERR

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.QUIT 4