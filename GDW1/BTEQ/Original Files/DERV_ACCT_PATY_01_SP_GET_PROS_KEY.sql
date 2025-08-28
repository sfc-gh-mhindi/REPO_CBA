.RUN FILE=%%BTEQ_LOGON_SCRIPT%%
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

.SET QUIET OFF
.SET ECHOREQ ON
.SET FORMAT OFF
.SET WIDTH 120

------------------------------------------------------------------------------
-- Object Name             :  DERV_ACCT_PATY_01_SP_GET_PROS_KEY.sql
-- Object Type             :  BTEQ

--                           
-- Description             :  Get process key for the batch key and the date  
--     
--
--  Ver  Date       Modified By            Description
--  ---- ---------- ---------------------- -----------------------------------
--  1.0  24/07/2013 Helen Zak              Initial Version
------------------------------------------------------------------------------
--
-- This info is for CBM use only
-- $LastChangedBy: zakhe $
-- $LastChangedDate: 2013-07-25 12:29:28 +1000 (Thu, 25 Jul 2013) $
-- $LastChangedRevision: 12363 $
--

  

-- Remove the existing pros key  and btch date files
.OS rm /cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_PROS_KEY.txt
.OS rm /cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_BTCH_DATE.txt

-- First get batch key and the date that we just inserted into UTIL_BTCH_ISAC 

.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_BTCH_KEY.txt
.EXPORT DATA FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_BTCH_DATE.txt
USING 
( 
  FILLER CHAR(24),
  BTCHKEY CHAR(10)) 
  SELECT  CAST(CAST(BTCH_KEY_I AS INTEGER) AS CHAR(10))
         ,CAST(BTCH_RUN_D AS CHAR(10))
        
    FROM %%VTECH%%.UTIL_BTCH_ISAC
  WHERE BTCH_KEY_I = :BTCHKEY;

 
.EXPORT RESET 

 .IF ERRORCODE <> 0    THEN .GOTO EXITERR

 
-- Using batch key and the date, obtain pros key

.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_BTCH_DATE.txt          
.EXPORT DATA FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_PROS_KEY.txt

USING 

(  FILLER CHAR(4)
   ,BTCHKEY CHAR(10)
   ,EXTR_D CHAR(10)
   )
CALL %%STARMACRDB%%.SP_GET_PROS_KEY(        
  '%%GDW_USER%%',           -- From the config.pass parameters
  '%%SRCE_SYST_M%%',          
  '%%SRCE_M%%',           
  '%%PSST_TABLE_M%%',
  CAST(TRIM(:BTCHKEY) as DECIMAL(10,0)),
  CAST(:EXTR_D AS DATE) (FORMAT'YYYYMMDD')(CHAR(8)),            
  '%%RSTR_F%%',                -- Restart Flag
  'TD',                     
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
 
