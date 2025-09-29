.RUN FILE=%%BTEQ_LOGON_SCRIPT%%
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

.SET QUIET OFF
.SET ECHOREQ ON
.SET FORMAT OFF
.SET WIDTH 120

------------------------------------------------------------------------------
--  Script name:    DERV_ACCT_PATY_99_SP_COMT_PROS_KEY.sql
--
--  Ver  Date       Modified By            Description
--  ---- ---------- ---------------------- -----------------------------------
--  1.0  25/07/2013 Helen Zak              Initial Version
--  1.1  31/07/2013 Megan Disch            Pros key starts @ posn 4
--  1.2  08/08/2013 Helen Zak              C0714578 - post-implementation changes
--                                         read correct file and use it to update
--                                         UTIL_PROS_ISAC 
------------------------------------------------------------------------------
--
-- This info is for CBM use only
-- $LastChangedBy: zakhe $
-- $LastChangedDate: 2013-08-12 17:18:25 +1000 (Mon, 12 Aug 2013) $
-- $LastChangedRevision: 12425 $
--

.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_PROS_KEY_DATE.txt
USING 
( 
  FILLER CHAR(4),
  PROSKEY CHAR(10),
  EXTR_D  CHAR(10) )
UPDATE %%STARDATADB%%.UTIL_PROS_ISAC           
SET
   SUCC_F  = 'Y'
  ,COMT_F = 'Y'
  ,COMT_S = CAST(CAST(current_timestamp as CHAR(20)) as TIMESTAMP(0))
WHERE
  PROS_KEY_I = CAST(trim(:PROSKEY) as DECIMAL(10,0)) 
  AND SRCE_SYST_M = '%%SRCE_M%%'
  AND TRGT_M = '%%PSST_TABLE_M%%'
  AND BTCH_RUN_D = CAST(:EXTR_D  AS DATE) (FORMAT'YYYY-MM-DD')(CHAR(10))
  
;  


.EXPORT RESET                  
		
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

.QUIT 0
.LOGOFF

.LABEL EXITERR
.QUIT 1
.LOGOFF
.EXIT 
 
