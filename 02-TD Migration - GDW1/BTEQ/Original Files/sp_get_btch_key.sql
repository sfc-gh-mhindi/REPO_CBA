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
-- $LastChangedDate: 2013-09-02 13:49:23 +1000 (Mon, 02 Sep 2013) $
-- $LastChangedRevision: 12581 $
--

.EXPORT RESET     

-- Remove the existing Batch Key file
.OS rm /cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_BTCH_KEY.txt

.EXPORT DATA FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_BTCH_KEY.txt

.SET FORMAT OFF

CALL %%STARMACRDB%%.SP_GET_BTCH_KEY(     
  '%%SRCE_SYST_M%%'
  ,CAST(%%INDATE%% as date format'yyyymmdd')              
  ,IBATCHKEY (CHAR(80))
); 

/* NOTE: BTCH_KEY is returned as a string in the format where     */
/* First 24 characters ignored then next 10 are the actual number */

.EXPORT RESET   

-- Remove the existing DATE file
.OS rm /cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_DATE.txt

.EXPORT DATA FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_DATE.txt
Select
  (CAST(%%INDATE%% as date format'yyyymmdd'))(CHAR(8))
;

.SET FORMAT OFF
		
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

.QUIT 0
.LOGOFF

.LABEL EXITERR
.QUIT 1
.LOGOFF
.EXIT 
 
