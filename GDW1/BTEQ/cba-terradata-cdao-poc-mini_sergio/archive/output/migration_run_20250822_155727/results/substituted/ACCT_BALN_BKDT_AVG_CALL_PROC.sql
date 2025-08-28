.RUN FILE=%%BTEQ_LOGON_SCRIPT%%
.IF ERRORCODE <> 0 THEN .GOTO EXITERR


.SET QUIET OFF
.SET ECHOREQ ON
.SET FORMAT OFF
.SET WIDTH 120
----------------------------------------------------------------------
-- $LastChangedBy: 
-- $LastChangedDate: 2012-02-28 09:08:46 +1100 (Tue, 28 Feb 2012) $
-- $LastChangedRevision: 9219 $
----------------------------------------------------------------------
------------------------------------------------------------------------------
--
--  Description :Process to calculate the Monthly Average balance.  
--This is sourcing from ACCT BALN BKDT.
--
--   Ver  Date       Modified By            Description
--  ---- ---------- ---------------------- -----------------------------------
--  1.0  2011-10-05 Suresh Vajapeyajula               Initial Version
------------------------------------------------------------------------------

CALL %%CAD_PROD_MACRO%%.SP_CALC_AVRG_DAY_BALN_BKDT (
CAST(ADD_MONTHS(CURRENT_DATE, -1) AS DATE FORMAT 'YYYYMMDD'));
 
.IF ERRORCODE <> 0 THEN .GOTO EXITERR

.QUIT 0
.LOGOFF

.LABEL EXITERR
.QUIT ERRORCODE
.LOGOFF
.EXIT