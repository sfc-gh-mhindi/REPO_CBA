.RUN FILE=%%BTEQ_LOGON_SCRIPT%%
.IF ERRORCODE <> 0 THEN .GOTO EXITERR

.SET QUIET OFF
.SET ECHOREQ ON
.SET FORMAT OFF
.SET WIDTH 120
----------------------------------------------------------------------
-- $LastChangedBy: vajapes $
-- $LastChangedDate: 2012-02-28 09:09:17 +1100 (Tue, 28 Feb 2012) $
-- $LastChangedRevision: 9222 $
----------------------------------------------------------------------
------------------------------------------------------------------------------
--
--  Description : Populate ACCT's into ACCT BALN with the modified adjustments
--
--   Ver  Date       Modified By            Description
--  ---- ---------- ---------------------- -----------------------------------
--  1.0  2011-10-05 Suresh Vajapeyajula               Initial Version
------------------------------------------------------------------------------

/*Inserting the data  into ACCT_BALN_BKDT from  ACCT_BALN_BKDT_STG2*/
INSERT INTO %%CAD_PROD_DATA%%.ACCT_BALN_BKDT
(
ACCT_I,                        
BALN_TYPE_C,                   
CALC_FUNC_C,                   
TIME_PERD_C,                   
BALN_A,                        
CALC_F,                        
SRCE_SYST_C,                   
ORIG_SRCE_SYST_C,              
LOAD_D,                        
BKDT_EFFT_D,                   
BKDT_EXPY_D,                  
PROS_KEY_EFFT_I,               
PROS_KEY_EXPY_I,               
BKDT_PROS_KEY_I
)
SELECT 
ACCT_I,                        
BALN_TYPE_C,                   
CALC_FUNC_C,                   
TIME_PERD_C,                   
BALN_A,                        
CALC_F,                        
SRCE_SYST_C,                   
ORIG_SRCE_SYST_C,              
LOAD_D,                        
BKDT_EFFT_D,                   
BKDT_EXPY_D,                  
PROS_KEY_EFFT_I,               
PROS_KEY_EXPY_I,               
BKDT_PROS_KEY_I
FROM
%%DDSTG%%.ACCT_BALN_BKDT_STG2;

.IF ERRORCODE <> 0 THEN .GOTO EXITERR

.QUIT 0
.LOGOFF
.EXIT

.LABEL EXITERR
.QUIT 1
.LOGOFF
.EXIT