.RUN FILE=%%BTEQ_LOGON_SCRIPT%%
.IF ERRORCODE <> 0 THEN .GOTO EXITERR

.SET QUIET OFF
.SET ECHOREQ ON
.SET FORMAT OFF
.SET WIDTH 120
----------------------------------------------------------------------
-- $LastChangedBy: vajapes $
-- $LastChangedDate: 2012-02-28 09:08:57 +1100 (Tue, 28 Feb 2012) $
-- $LastChangedRevision: 9220 $
----------------------------------------------------------------------
------------------------------------------------------------------------------
--
--  Description :Delete Accts from ACCT BALN so that the modified data can be inserted in next step
--
--   Ver  Date       Modified By            Description
--  ---- ---------- ---------------------- -----------------------------------
--  1.0  2011-10-05 Suresh Vajapeyajula               Initial Version
------------------------------------------------------------------------------


DELETE BAL
/* Deleting the records from the ACCT_BALN_BKDT table. These records are modified 
as a result of applying adjustment and so will be resinserted from STG2 at next step*/
FROM
 %%CAD_PROD_DATA%%.ACCT_BALN_BKDT BAL,
 PDDSTG.ACCT_BALN_BKDT_STG1 STG1
 WHERE 
STG1.ACCT_I = BAL.ACCT_I    
AND STG1.BALN_TYPE_C = BAL.BALN_TYPE_C                    
AND STG1.CALC_FUNC_C = BAL.CALC_FUNC_C                   
AND STG1.TIME_PERD_C = BAL.TIME_PERD_C                   
AND STG1.BKDT_EFFT_D = BAL.BKDT_EFFT_D                        
AND STG1.BKDT_EXPY_D = BAL.BKDT_EXPY_D                        
AND STG1.BALN_A = BAL.BALN_A                        
AND STG1.CALC_F = BAL.CALC_F                        
AND COALESCE(STG1.PROS_KEY_EFFT_I,0) = COALESCE(BAL.PROS_KEY_EFFT_I,0)
AND COALESCE(STG1.PROS_KEY_EXPY_I,0) = COALESCE(BAL.PROS_KEY_EXPY_I,0);

.IF ERRORCODE <> 0 THEN .GOTO EXITERR

.QUIT 0
.LOGOFF
.EXIT

.LABEL EXITERR
.QUIT 1
.LOGOFF
.EXIT
