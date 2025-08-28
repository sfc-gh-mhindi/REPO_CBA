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
-- $LastChangedDate: 2013-07-03 15:57:40 +1000 (Wed, 03 Jul 2013) $
-- $LastChangedRevision: 12229 $
--

.EXPORT RESET     

-- Remove the existing DATE file
.OS rm /cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_%%TBSHORT%%PROS_KEY.txt

.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_BTCH_KEY.txt

.EXPORT DATA FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_%%TBSHORT%%PROS_KEY.txt

USING 
( 
  FILLER CHAR(24),
  BTCHKEY CHAR(10) )
CALL %%STARMACRDB%%.SP_GET_PROS_KEY(        
  '%%GDW_USER%%',           -- From the config.pass parameters
  '%%SRCE_SYST_M%%',          
  '%%SRCE_M%%',           
  '%%PSST_TABLE_M%%',           
  CAST(trim(:BTCHKEY) as DECIMAL(10,0)),
  CAST(%%INDATE%% as DATE FORMAT'YYYYMMDD'),            
  '%%RSTR_F%%',                -- Restart Flag
  'MVS',                     
  IPROCESSKEY (CHAR(100))
);  

.EXPORT RESET                  
                		
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

.QUIT 0
.LOGOFF

.LABEL EXITERR
.QUIT 1
.LOGOFF
.EXIT 
 
