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
  # $LastChangedDate: 2012-02-28 09:09:54 +1100 (Tue, 28 Feb 2012) $
  # $LastChangedRevision: 9226 $
  #--------------------------------------------------------------------
  #----------------------------------------------------------------------------
  #
  #  Description :  Updating  UTIL PROS ISAC with the status.
  #
  #   Ver  Date       Modified By            Description
  #  ---- ---------- ---------------------- -----------------------------------
  #  1.0  2011-10-05 Suresh Vajapeyajula     Initial Version
  #----------------------------------------------------------------------------
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '22' COLUMN '0' OF THE SOURCE CODE STARTING AT 'UPDATE'. EXPECTED 'Update' GRAMMAR. LAST MATCHING TOKEN WAS 'UPDATE' ON LINE '22' COLUMN '0'. FAILED TOKEN WAS '%' ON LINE '22' COLUMN '7'. **
------------------------------------------------------------------------
---- $LastChangedBy: vajapes $
---- $LastChangedDate: 2012-02-28 09:09:54 +1100 (Tue, 28 Feb 2012) $
---- $LastChangedRevision: 9226 $
------------------------------------------------------------------------
--------------------------------------------------------------------------------
----
----  Description :  Updating  UTIL PROS ISAC with the status.
----
----   Ver  Date       Modified By            Description
----  ---- ---------- ---------------------- -----------------------------------
----  1.0  2011-10-05 Suresh Vajapeyajula     Initial Version
--------------------------------------------------------------------------------

--UPDATE %%CAD_PROD_DATA%%.UTIL_PROS_ISAC
--FROM
--(SELECT COUNT(*) FROM
--%%DDSTG%%.ACCT_BALN_BKDT_STG2)A(INS_CNT),
--(SELECT COUNT(*) FROM
--%%DDSTG%%.ACCT_BALN_BKDT_STG1)B(DEL_CNT)
--SET
--        COMT_F = 'Y',
--	SUCC_F = 'Y',
--	COMT_S = CURRENT_TIMESTAMP(0),
--	SYST_INS_Q = A.INS_CNT,
--	SYST_DEL_Q = B.DEL_CNT
--WHERE
--CONV_M='CAD_X01_ACCT_BALN_BKDT'
--AND PROS_KEY_I = (SELECT MAX(PROS_KEY_I) FROM %%VTECH%%.UTIL_PROS_ISAC
--WHERE CONV_M='CAD_X01_ACCT_BALN_BKDT')
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