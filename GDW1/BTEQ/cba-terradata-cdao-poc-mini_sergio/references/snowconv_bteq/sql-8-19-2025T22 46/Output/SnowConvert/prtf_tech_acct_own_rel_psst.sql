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

--------------------------------------------------------------------------------
---- Object Name             :  prtf_tech_acct_own_rel_psst.sql
---- Object Type             :  BTEQ
----                           
---- Description             :  Persist rows for DERV_PRTF_ACCT_OWN view

--------------------------------------------------------------------------------
----   Modification history

--------------------------------------------------------------------------------                       --
----  Ver  Date                   Modified By        Description
----  ---- -----------------    ------------             ---------------------------------------
----  1.0  09/01/2014    Helen Zak            Initial Version

--------------------------------------------------------------------------------
---- This info is for CBM use only
---- $LastChangedBy: zakhe $
---- $LastChangedDate: 2014-01-10 14:56:29 +1100 (Fri, 10 Jan 2014) $
---- $LastChangedRevision: 13159 $
----
--!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 30 OF FILE: /Users/psundaram/Documents/prjs/cba/agentic/bteq/prtf_tech_acct_own_rel_psst.sql ***/!!!

--Delete
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '30' COLUMN '7' OF THE SOURCE CODE STARTING AT 'from'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'from' ON LINE '30' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '30' COLUMN '12'. **
--       from %%STARDATADB%%.DERV_PRTF_ACCT_OWN_REL All
;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '36' COLUMN '2' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '36' COLUMN '2'. **
---- Collect stats after deletion so that the optimiser "knows" that the table is empty

-- COLLECT STATS  %%STARDATADB%%.DERV_PRTF_ACCT_OWN_REL
                                                     ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '40' COLUMN '0' OF THE SOURCE CODE STARTING AT 'Insert'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'into' ON LINE '40' COLUMN '7'. **
--Insert into %%STARDATADB%%.DERV_PRTF_ACCT_OWN_REL
--(  ACCT_I
-- , INT_GRUP_I
-- , DERV_PRTF_CATG_C
-- , DERV_PRTF_CLAS_C
-- , DERV_PRTF_TYPE_C
-- , PRTF_ACCT_VALD_FROM_D
-- , PRTF_ACCT_VALD_TO_D
-- , PRTF_ACCT_EFFT_D
-- , PRTF_ACCT_EXPY_D
-- , PRTF_OWN_VALD_FROM_D
-- , PRTF_OWN_VALD_TO_D
-- , PRTF_OWN_EFFT_D
-- , PRTF_OWN_EXPY_D
-- , PTCL_N
-- , REL_MNGE_I
-- , PRTF_CODE_X
-- , DERV_PRTF_ROLE_C
-- , ROLE_PLAY_TYPE_X
-- , ROLE_PLAY_I
-- , SRCE_SYST_C
-- , ROW_SECU_ACCS_C
--)
--SELECT
--      PP4.ACCT_I AS ACCT_I
--    , PP4.INT_GRUP_I AS INT_GRUP_I
--    , PP4.DERV_PRTF_CATG_C AS DERV_PRTF_CATG_C
--    , PP4.DERV_PRTF_CLAS_C AS DERV_PRTF_CLAS_C
--    , PP4.DERV_PRTF_TYPE_C AS DERV_PRTF_TYPE_C
--    , PP4.VALD_FROM_D AS PRTF_ACCT_VALD_FROM_D
--    , PP4.VALD_TO_D AS PRTF_ACCT_VALD_TO_D
--    , PP4.EFFT_D AS PRTF_ACCT_EFFT_D
--    , PP4.EXPY_D AS PRTF_ACCT_EXPY_D
--    , PO4.VALD_FROM_D AS PRTF_OWN_VALD_FROM_D
--    , PO4.VALD_TO_D AS PRTF_OWN_VALD_TO_D
--    , PO4.EFFT_D AS PRTF_OWN_EFFT_D
--    , PO4.EXPY_D AS PRTF_OWN_EXPY_D
--    , PP4.PTCL_N AS PTCL_N
--    , PP4.REL_MNGE_I AS REL_MNGE_I
--    , PP4.PRTF_CODE_X AS PRTF_CODE_X
--    , PO4.DERV_PRTF_ROLE_C AS DERV_PRTF_ROLE_C
--    , PO4.ROLE_PLAY_TYPE_X AS ROLE_PLAY_TYPE_X
--    , PO4.ROLE_PLAY_I AS ROLE_PLAY_I
--    , PP4.SRCE_SYST_C AS SRCE_SYST_C
--    , PP4.ROW_SECU_ACCS_C AS ROW_SECU_ACCS_C

--FROM %%VTECH%%.DERV_PRTF_ACCT_REL PP4
--INNER JOIN %%VTECH%%.DERV_PRTF_OWN_REL PO4
--ON PO4.INT_GRUP_I = PP4.INT_GRUP_I
--AND PO4.DERV_PRTF_TYPE_C = PP4.DERV_PRTF_TYPE_C
                                                ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!


.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '94' COLUMN '1' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '94' COLUMN '1'. **
--COLLECT STATS  %%STARDATADB%%.DERV_PRTF_ACCT_OWN_REL
                                                    ;

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