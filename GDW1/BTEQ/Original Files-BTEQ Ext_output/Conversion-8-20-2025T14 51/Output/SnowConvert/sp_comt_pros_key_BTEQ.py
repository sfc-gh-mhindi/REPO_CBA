#*** Generated code is based on the SnowConvert Python Helpers version 2.0.6 ***
 
import os
import sys
import snowconvert.helpers
from snowconvert.helpers import Export
from snowconvert.helpers import exec
from snowconvert.helpers import BeginLoading
con = None
def main():
  snowconvert.helpers.configure_log()
  con = snowconvert.helpers.log_on()

  for statement in snowconvert.helpers.readrun(fr"%%BTEQ_LOGON_SCRIPT%%"):
    eval(statement)

  if snowconvert.helpers.error_code != 0:
    EXITERR()
    return
  #.SET QUIET OFF
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'Quiet' NODE ***/!!!
  None
  #.SET ECHOREQ ON
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'Echoreq' NODE ***/!!!
  None
  #.SET FORMAT OFF
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'Format' NODE ***/!!!
  None
  #** SSC-EWI-TD0005 - THE STATEMENT WAS CONVERTED BUT ITS FUNCTIONALITY IS NOT IMPLEMENTED YET **
  Export.width(120)
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '23' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '23' COLUMN '2'. **
  #--IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_%%TBSHORT%%PROS_KEY.txt
  #--USING
  #--(
  #--  FILLER CHAR(13),
  #--  PROS_KEY CHAR(11) )
  #--UPDATE %%STARDATADB%%.UTIL_PROS_ISAC
  #--SET
  #--   SUCC_F  = 'Y'
  #--  ,COMT_F = 'Y'
  #--  ,COMT_S = CAST(CAST(current_timestamp as CHAR(20)) as TIMESTAMP(0))
  #--WHERE
  #--  PROS_KEY_I = CAST(trim(:PROS_KEY) as DECIMAL(10,0))
  #--  AND SRCE_SYST_M = '%%SRCE_M%%'
  #--  AND TRGT_M = '%%PSST_TABLE_M%%'
  #--  AND BTCH_RUN_D = CAST(%%INDATE%% as DATE FORMAT'YYYYMMDD')
   
  #** SSC-EWI-TD0005 - THE STATEMENT WAS CONVERTED BUT ITS FUNCTIONALITY IS NOT IMPLEMENTED YET **
  Export.reset()

  if snowconvert.helpers.error_code != 0:
    EXITERR()
    return
  snowconvert.helpers.quit_application(0)
  #** SSC-FDM-0027 - REMOVED NEXT STATEMENT, NOT APPLICABLE IN SNOWFLAKE. LOGOFF **
  #.LOGOFF
   
  EXITERR()
  snowconvert.helpers.quit_application()
def EXITERR():
  snowconvert.helpers.quit_application(1)
  #** SSC-FDM-0027 - REMOVED NEXT STATEMENT, NOT APPLICABLE IN SNOWFLAKE. LOGOFF **
  #.LOGOFF
   
  snowconvert.helpers.quit_application()

if __name__ == "__main__":
  main()