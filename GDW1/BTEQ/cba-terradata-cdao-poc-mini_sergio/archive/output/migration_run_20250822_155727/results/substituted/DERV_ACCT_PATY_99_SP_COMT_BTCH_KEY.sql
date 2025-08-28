.RUN FILE=%%BTEQ_LOGON_SCRIPT%%
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

.SET QUIET OFF
.SET ECHOREQ ON
.SET FORMAT OFF
.SET WIDTH 120

------------------------------------------------------------------------------
--  Update the Batch record to show successful completion
--
--  Ver  Date       Modified By            Description
--  ---- ---------- ---------------------- -----------------------------------
--  1.0  25/07/2013 Helen Zak            Initial Version
------------------------------------------------------------------------------
--
-- This info is for CBM use only
-- $LastChangedBy: zakhe $
-- $LastChangedDate: 2013-07-25 13:39:43 +1000 (Thu, 25 Jul 2013) $
-- $LastChangedRevision: 12366 $
--

.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_BTCH_KEY.txt
USING 
( 
  FILLER CHAR(24),
  BTCH_KEY CHAR(10) )
UPDATE %%STARDATADB%%.UTIL_BTCH_ISAC           
SET
   BTCH_STUS_C = 'COMT'
  ,STUS_CHNG_S = CAST(CAST(current_timestamp as CHAR(20)) as TIMESTAMP(0))
WHERE
  BTCH_KEY_I = CAST(trim(:BTCH_KEY) as DECIMAL(10,0)) 
;  

.EXPORT RESET                  
		
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

.QUIT 0
.LOGOFF

.LABEL EXITERR
.QUIT 1
.LOGOFF
.EXIT 
 
