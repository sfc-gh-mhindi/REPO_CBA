.RUN FILE=%%BTEQ_LOGON_SCRIPT%%
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

.SET QUIET OFF
.SET ECHOREQ ON
.SET FORMAT OFF
.SET WIDTH 120

------------------------------------------------------------------------------
--
--
--  Ver  Date       Modified By            Description
--  ---- ---------- ---------------------- -----------------------------------
--  1.0  11/06/2013 T Jelliffe             Initial Version
------------------------------------------------------------------------------
--
-- This info is for CBM use only
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-04 12:06:00 +1000 (Thu, 04 Jul 2013) $
-- $LastChangedRevision: 12239 $
--

.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_%%TBSHORT%%PROS_KEY.txt
USING 
( 
  FILLER CHAR(13),
  PROS_KEY CHAR(11) )
UPDATE %%STARDATADB%%.UTIL_PROS_ISAC           
SET
   SUCC_F  = 'Y'
  ,COMT_F = 'Y'
  ,COMT_S = CAST(CAST(current_timestamp as CHAR(20)) as TIMESTAMP(0))
WHERE
  PROS_KEY_I = CAST(trim(:PROS_KEY) as DECIMAL(10,0)) 
  AND SRCE_SYST_M = '%%SRCE_M%%'
  AND TRGT_M = '%%PSST_TABLE_M%%'
  AND BTCH_RUN_D = CAST(%%INDATE%% as DATE FORMAT'YYYYMMDD')
;  


.EXPORT RESET                  
		
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

.QUIT 0
.LOGOFF

.LABEL EXITERR
.QUIT 1
.LOGOFF
.EXIT 
 
