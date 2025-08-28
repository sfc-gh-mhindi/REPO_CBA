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
  # $LastChangedBy: vajapes $
  # $LastChangedDate: 2012-02-28 09:09:17 +1100 (Tue, 28 Feb 2012) $
  # $LastChangedRevision: 9222 $
  #--------------------------------------------------------------------
  #----------------------------------------------------------------------------
  #
  #  Description : Populate ACCT's into ACCT BALN with the modified adjustments
  #
  #   Ver  Date       Modified By            Description
  #  ---- ---------- ---------------------- -----------------------------------
  #  1.0  2011-10-05 Suresh Vajapeyajula               Initial Version
  #----------------------------------------------------------------------------
  #Inserting the data  into ACCT_BALN_BKDT from  ACCT_BALN_BKDT_STG2
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '23' COLUMN '0' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '23' COLUMN '7'. **
------------------------------------------------------------------------
---- $LastChangedBy: vajapes $
---- $LastChangedDate: 2012-02-28 09:09:17 +1100 (Tue, 28 Feb 2012) $
---- $LastChangedRevision: 9222 $
------------------------------------------------------------------------
--------------------------------------------------------------------------------
----
----  Description : Populate ACCT's into ACCT BALN with the modified adjustments
----
----   Ver  Date       Modified By            Description
----  ---- ---------- ---------------------- -----------------------------------
----  1.0  2011-10-05 Suresh Vajapeyajula               Initial Version
--------------------------------------------------------------------------------

--/*Inserting the data  into ACCT_BALN_BKDT from  ACCT_BALN_BKDT_STG2*/
--INSERT INTO %%CAD_PROD_DATA%%.ACCT_BALN_BKDT
--(
--ACCT_I,
--BALN_TYPE_C,
--CALC_FUNC_C,
--TIME_PERD_C,
--BALN_A,
--CALC_F,
--SRCE_SYST_C,
--ORIG_SRCE_SYST_C,
--LOAD_D,
--BKDT_EFFT_D,
--BKDT_EXPY_D,
--PROS_KEY_EFFT_I,
--PROS_KEY_EXPY_I,
--BKDT_PROS_KEY_I
--)
--SELECT
--ACCT_I,
--BALN_TYPE_C,
--CALC_FUNC_C,
--TIME_PERD_C,
--BALN_A,
--CALC_F,
--SRCE_SYST_C,
--ORIG_SRCE_SYST_C,
--LOAD_D,
--BKDT_EFFT_D,
--BKDT_EXPY_D,
--PROS_KEY_EFFT_I,
--PROS_KEY_EXPY_I,
--BKDT_PROS_KEY_I
--FROM
--%%DDSTG%%.ACCT_BALN_BKDT_STG2
""")

  if snowconvert.helpers.error_code != 0:
EXITERR()
return
  snowconvert.helpers.quit_application(0)
  #** SSC-FDM-0027 - REMOVED NEXT STATEMENT, NOT APPLICABLE IN SNOWFLAKE. LOGOFF **
  #.LOGOFF
   
  snowconvert.helpers.quit_application()
  EXITERR()
  snowconvert.helpers.quit_application()
def EXITERR():
  snowconvert.helpers.quit_application(1)
  #** SSC-FDM-0027 - REMOVED NEXT STATEMENT, NOT APPLICABLE IN SNOWFLAKE. LOGOFF **
  #.LOGOFF
   
  snowconvert.helpers.quit_application()

if __name__ == "__main__":
  main()