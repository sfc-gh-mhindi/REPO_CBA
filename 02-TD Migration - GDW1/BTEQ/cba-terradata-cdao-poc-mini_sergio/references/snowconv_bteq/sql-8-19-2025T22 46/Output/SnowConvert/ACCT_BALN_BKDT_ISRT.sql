!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.RUN FILE=%%BTEQ_LOGON_SCRIPT%%

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.IF ERRORCODE <> 0 THEN .GOTO EXITERR

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.SET QUIET OFF

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.SET ECHOREQ ON

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.SET FORMAT OFF

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.SET WIDTH 120
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '23' COLUMN '0' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '23' COLUMN '7'. **
------------------------------------------------------------------------
---- $LastChangedBy: vajapes $
---- $LastChangedDate: 2012-02-28 09:09:17 +1100 (Tue, 28 Feb 2012) $
---- $LastChangedRevision: 9222 $
------------------------------------------------------------------------
--------------------------------------------------------------------------------
----
----  Description : Populate ACCT's into ACCT BALN with the modified adjustments
----
----   Ver  Date       Modified By            Description
----  ---- ---------- ---------------------- -----------------------------------
----  1.0  2011-10-05 Suresh Vajapeyajula               Initial Version
--------------------------------------------------------------------------------

--/*Inserting the data  into ACCT_BALN_BKDT from  ACCT_BALN_BKDT_STG2*/
--INSERT INTO %%CAD_PROD_DATA%%.ACCT_BALN_BKDT
--(
--ACCT_I,
--BALN_TYPE_C,
--CALC_FUNC_C,
--TIME_PERD_C,
--BALN_A,
--CALC_F,
--SRCE_SYST_C,
--ORIG_SRCE_SYST_C,
--LOAD_D,
--BKDT_EFFT_D,
--BKDT_EXPY_D,
--PROS_KEY_EFFT_I,
--PROS_KEY_EXPY_I,
--BKDT_PROS_KEY_I
--)
--SELECT
--ACCT_I,
--BALN_TYPE_C,
--CALC_FUNC_C,
--TIME_PERD_C,
--BALN_A,
--CALC_F,
--SRCE_SYST_C,
--ORIG_SRCE_SYST_C,
--LOAD_D,
--BKDT_EFFT_D,
--BKDT_EXPY_D,
--PROS_KEY_EFFT_I,
--PROS_KEY_EXPY_I,
--BKDT_PROS_KEY_I
--FROM
--%%DDSTG%%.ACCT_BALN_BKDT_STG2
                             ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.IF ERRORCODE <> 0 THEN .GOTO EXITERR

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.QUIT 0

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.LOGOFF

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.EXIT

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.LABEL EXITERR

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.QUIT 1

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.LOGOFF

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.EXIT