 .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
.IF ERRORCODE <> 0 THEN .GOTO EXITERR

.SET QUIET OFF
.SET ECHOREQ ON
.SET FORMAT OFF
.SET WIDTH 120
----------------------------------------------------------------------
-- $LastChangedBy: vajapes $
-- $LastChangedDate: 2012-02-28 09:09:54 +1100 (Tue, 28 Feb 2012) $
-- $LastChangedRevision: 9226 $
----------------------------------------------------------------------
------------------------------------------------------------------------------
--
--  Description :  Updating  UTIL PROS ISAC with the status.
--
--   Ver  Date       Modified By            Description
--  ---- ---------- ---------------------- -----------------------------------
--  1.0  2011-10-05 Suresh Vajapeyajula     Initial Version
------------------------------------------------------------------------------

UPDATE %%CAD_PROD_DATA%%.UTIL_PROS_ISAC
FROM
(SELECT COUNT(*) FROM 
%%DDSTG%%.ACCT_BALN_BKDT_STG2)A(INS_CNT),
(SELECT COUNT(*) FROM 
%%DDSTG%%.ACCT_BALN_BKDT_STG1)B(DEL_CNT)
SET  
        COMT_F = 'Y',
	SUCC_F = 'Y',
	COMT_S = CURRENT_TIMESTAMP(0),
	SYST_INS_Q = A.INS_CNT,
	SYST_DEL_Q = B.DEL_CNT
WHERE 
CONV_M='CAD_X01_ACCT_BALN_BKDT'
AND PROS_KEY_I = (SELECT MAX(PROS_KEY_I) FROM %%VTECH%%.UTIL_PROS_ISAC 
WHERE CONV_M='CAD_X01_ACCT_BALN_BKDT'); 

.IF ERRORCODE <> 0 THEN .GOTO EXITERR

.QUIT 0
.LOGOFF
.EXIT

.LABEL EXITERR
.QUIT 1
.LOGOFF
.EXIT