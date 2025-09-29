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
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '24' COLUMN '0' OF THE SOURCE CODE STARTING AT 'CALL'. EXPECTED 'Call' GRAMMAR. LAST MATCHING TOKEN WAS 'CALL' ON LINE '24' COLUMN '0'. FAILED TOKEN WAS '%' ON LINE '24' COLUMN '5'. **
------------------------------------------------------------------------
---- $LastChangedBy: 
---- $LastChangedDate: 2012-02-28 09:08:46 +1100 (Tue, 28 Feb 2012) $
---- $LastChangedRevision: 9219 $
------------------------------------------------------------------------
--------------------------------------------------------------------------------
----
----  Description :Process to calculate the Monthly Average balance.  
----This is sourcing from ACCT BALN BKDT.
----
----   Ver  Date       Modified By            Description
----  ---- ---------- ---------------------- -----------------------------------
----  1.0  2011-10-05 Suresh Vajapeyajula               Initial Version
--------------------------------------------------------------------------------

--CALL %%CAD_PROD_MACRO%%.SP_CALC_AVRG_DAY_BALN_BKDT (
--CAST(ADD_MONTHS(CURRENT_DATE, -1) AS DATE FORMAT 'YYYYMMDD'))
                                                             ;

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.IF ERRORCODE <> 0 THEN .GOTO EXITERR

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.QUIT 0

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.LOGOFF

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!

.LABEL EXITERR

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.QUIT ERRORCODE

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.LOGOFF

!!!RESOLVE EWI!!! /*** SSC-EWI-0040 - THE STATEMENT IS NOT SUPPORTED IN SNOWFLAKE ***/!!!
.EXIT