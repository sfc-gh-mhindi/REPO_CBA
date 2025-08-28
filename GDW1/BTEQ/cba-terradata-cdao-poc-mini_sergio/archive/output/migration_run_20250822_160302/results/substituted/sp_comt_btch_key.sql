.RUN FILE=${ETL_APP_BTEQ}/bteq_login.sql
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
--  1.0  11/06/2013 T Jelliffe             Initial Version
------------------------------------------------------------------------------
--
-- This info is for CBM use only
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-04 12:06:00 +1000 (Thu, 04 Jul 2013) $
-- $LastChangedRevision: 12239 $
--

.IMPORT VARTEXT FILE=/cba_app/CBMGDW/PROD/schedule/DERV_ACCT_PATY_BTCH_KEY.txt
USING 
( 
  FILLER CHAR(24),
  BTCH_KEY CHAR(10) )
UPDATE STAR_CAD_PROD_DATA.UTIL_BTCH_ISAC           
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
 
