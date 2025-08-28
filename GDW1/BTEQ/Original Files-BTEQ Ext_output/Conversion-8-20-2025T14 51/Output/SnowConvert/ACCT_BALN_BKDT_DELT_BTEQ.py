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
  # $LastChangedDate: 2012-02-28 09:08:57 +1100 (Tue, 28 Feb 2012) $
  # $LastChangedRevision: 9220 $
  #--------------------------------------------------------------------
  #----------------------------------------------------------------------------
  #
  #  Description :Delete Accts from ACCT BALN so that the modified data can be inserted in next step
  #
  #   Ver  Date       Modified By            Description
  #  ---- ---------- ---------------------- -----------------------------------
  #  1.0  2011-10-05 Suresh Vajapeyajula               Initial Version
  #----------------------------------------------------------------------------
  exec("""
    !!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 23 OF FILE: /Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/Original Files-BTEQ Ext/ACCT_BALN_BKDT_DELT.bteq ***/!!!
    DELETE BAL
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '26' COLUMN '1' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '26' COLUMN '1'. FAILED TOKEN WAS '%' ON LINE '27' COLUMN '2'. **
--/* Deleting the records from the ACCT_BALN_BKDT table. These records are modified 
--as a result of applying adjustment and so will be resinserted from STG2 at next step*/
--FROM
-- %%CAD_PROD_DATA%%.ACCT_BALN_BKDT BAL,
-- %%DDSTG%%.ACCT_BALN_BKDT_STG1 STG1
 WHERE
      STG1.ACCT_I = BAL.ACCT_I
      AND STG1.BALN_TYPE_C = BAL.BALN_TYPE_C
      AND STG1.CALC_FUNC_C = BAL.CALC_FUNC_C
      AND STG1.TIME_PERD_C = BAL.TIME_PERD_C
      AND STG1.BKDT_EFFT_D = BAL.BKDT_EFFT_D
      AND STG1.BKDT_EXPY_D = BAL.BKDT_EXPY_D
      AND STG1.BALN_A = BAL.BALN_A
      AND STG1.CALC_F = BAL.CALC_F
      AND COALESCE(STG1.PROS_KEY_EFFT_I, 0) = COALESCE(BAL.PROS_KEY_EFFT_I, 0)
      AND COALESCE(STG1.PROS_KEY_EXPY_I, 0) = COALESCE(BAL.PROS_KEY_EXPY_I, 0)
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