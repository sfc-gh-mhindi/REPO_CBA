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
  #--------------------------------------------------------------------
  # $LastChangedBy: 
  # $LastChangedDate: 2012-02-28 09:08:46 +1100 (Tue, 28 Feb 2012) $
  # $LastChangedRevision: 9219 $
  #--------------------------------------------------------------------
  #----------------------------------------------------------------------------
  #
  #  Description :Process to calculate the Monthly Average balance.  
  #This is sourcing from ACCT BALN BKDT.
  #
  #   Ver  Date       Modified By            Description
  #  ---- ---------- ---------------------- -----------------------------------
  #  1.0  2011-10-05 Suresh Vajapeyajula               Initial Version
  #----------------------------------------------------------------------------
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '24' COLUMN '0' OF THE SOURCE CODE STARTING AT 'CALL'. EXPECTED 'Call' GRAMMAR. LAST MATCHING TOKEN WAS 'CALL' ON LINE '24' COLUMN '0'. FAILED TOKEN WAS '%' ON LINE '24' COLUMN '5'. **
------------------------------------------------------------------------
---- $LastChangedBy: 
---- $LastChangedDate: 2012-02-28 09:08:46 +1100 (Tue, 28 Feb 2012) $
---- $LastChangedRevision: 9219 $
------------------------------------------------------------------------
--------------------------------------------------------------------------------
----
----  Description :Process to calculate the Monthly Average balance.  
----This is sourcing from ACCT BALN BKDT.
----
----   Ver  Date       Modified By            Description
----  ---- ---------- ---------------------- -----------------------------------
----  1.0  2011-10-05 Suresh Vajapeyajula               Initial Version
--------------------------------------------------------------------------------

--CALL %%CAD_PROD_MACRO%%.SP_CALC_AVRG_DAY_BALN_BKDT (
--CAST(ADD_MONTHS(CURRENT_DATE, -1) AS DATE FORMAT 'YYYYMMDD'))
""")

  if snowconvert.helpers.error_code != 0:
EXITERR()
return
  snowconvert.helpers.quit_application(0)
  #** SSC-FDM-0027 - REMOVED NEXT STATEMENT, NOT APPLICABLE IN SNOWFLAKE. LOGOFF **
  #.LOGOFF
   
  EXITERR()
  snowconvert.helpers.quit_application()
def EXITERR():
  snowconvert.helpers.quit_application(snowconvert.helpers.error_code)
  #** SSC-FDM-0027 - REMOVED NEXT STATEMENT, NOT APPLICABLE IN SNOWFLAKE. LOGOFF **
  #.LOGOFF
   
  snowconvert.helpers.quit_application()

if __name__ == "__main__":
  main()