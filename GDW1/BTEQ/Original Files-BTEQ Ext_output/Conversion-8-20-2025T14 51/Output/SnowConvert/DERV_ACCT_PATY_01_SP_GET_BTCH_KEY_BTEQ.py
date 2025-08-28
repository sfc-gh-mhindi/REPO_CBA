#*** Generated code is based on the SnowConvert Python Helpers version 2.0.6 ***
 
import os
import sys
import snowconvert.helpers
from snowconvert.helpers import Export
from snowconvert.helpers import exec
from snowconvert.helpers import BeginLoading
con = None
#** SSC-FDM-TD0022 - SHELL VARIABLES FOUND, RUNNING THIS CODE IN A SHELL SCRIPT IS REQUIRED **
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
  #----------------------------------------------------------------------------
  # Object Name             :  DERV_ACCT_PATY_01_SP_GET_BTCH_KEY.sql
  # Object Type             :  BTEQ
  #                           
  # Description             :  Call a stored procedure to obtain batch key for the extract date  
  #     
  #
  #  Ver  Date       Modified By            Description
  #  ---- ---------- ---------------------- -----------------------------------
  #  1.0  24/07/2013 Helen Zak              Initial Version
  #----------------------------------------------------------------------------
  #
  # This info is for CBM use only
  # $LastChangedBy: zakhe $
  # $LastChangedDate: 2013-07-25 12:29:28 +1000 (Thu, 25 Jul 2013) $
  # $LastChangedRevision: 12363 $
  #
  #** SSC-EWI-TD0005 - THE STATEMENT WAS CONVERTED BUT ITS FUNCTIONALITY IS NOT IMPLEMENTED YET **
  Export.reset()
  # Remove the existing batch key file
  snowconvert.helpers.os("""rm /cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_BTCH_KEY.txt""")
  #** SSC-EWI-TD0005 - THE STATEMENT WAS CONVERTED BUT ITS FUNCTIONALITY IS NOT IMPLEMENTED YET **
  Export.report(fr"/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_BTCH_KEY.txt", ",")
  #.SET FORMAT OFF
  !!!RESOLVE EWI!!! /*** SSC-EWI-0073 - PENDING FUNCTIONAL EQUIVALENCE REVIEW FOR 'Format' NODE ***/!!!
  None
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '40' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '40' COLUMN '2'. **
  #--IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
  #--USING
  #--( EXTR_D VARCHAR(10) )
  #--CALL %%STARMACRDB%%.SP_GET_BTCH_KEY(
  #--  '%%SRCE_SYST_M%%'
  #--  ,CAST(:EXTR_D AS DATE) (FORMAT'YYYYMMDD')(CHAR(8))
  #--  ,IBATCHKEY (CHAR(80))
  #--)
   
  # NOTE: BTCH_KEY is returned as a string in the format where     
  # First 24 characters ignored then next 10 are the actual number 
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