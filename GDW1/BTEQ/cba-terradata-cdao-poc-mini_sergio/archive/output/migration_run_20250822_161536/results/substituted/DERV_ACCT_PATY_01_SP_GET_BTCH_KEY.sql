.RUN FILE=${ETL_APP_BTEQ}/bteq_login.sql
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

.SET QUIET OFF
.SET ECHOREQ ON
.SET FORMAT OFF
.SET WIDTH 120

------------------------------------------------------------------------------
-- Object Name             :  DERV_ACCT_PATY_01_SP_GET_BTCH_KEY.sql
-- Object Type             :  BTEQ

--                           
-- Description             :  Call a stored procedure to obtain batch key for the extract date  
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

.EXPORT RESET     

-- Remove the existing batch key file
.OS rm /cba_app/CBMGDW/PROD/schedule/DERV_ACCT_PATY_BTCH_KEY.txt

.EXPORT DATA FILE=/cba_app/CBMGDW/PROD/schedule/DERV_ACCT_PATY_BTCH_KEY.txt

.SET FORMAT OFF

-- Get extract date as set by the datawatcher and get a batch key for that date


.IMPORT VARTEXT FILE=/cba_app/CBMGDW/PROD/schedule/DERV_ACCT_PATY_extr_date.txt
USING 
( EXTR_D VARCHAR(10) )
CALL STAR_CAD_PROD_MACRO.SP_GET_BTCH_KEY(     
  'DERIVED_ACCOUNT_PARTY'
  ,CAST(:EXTR_D AS DATE) (FORMAT'YYYYMMDD')(CHAR(8))              
  ,IBATCHKEY (CHAR(80))
); 

/* NOTE: BTCH_KEY is returned as a string in the format where     */
/* First 24 characters ignored then next 10 are the actual number */

.EXPORT RESET              
		
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

.QUIT 0
.LOGOFF

.LABEL EXITERR
.QUIT 1
.LOGOFF
.EXIT 
 
