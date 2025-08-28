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
  #
  #  Ver  Date       Modified By        Description
  #  ---- ---------- ------------------ ---------------------------------------
  #  1.0  11/06/2013 T Jelliffe         Initial Version
  #  1.1  11/07/2013 T Jelliffe         Use PROS_KEY_EFFT_I prevent self join
  #  1.2  12/07/2013 T Jelliffe         Time period reduced 15 to 3 years
  #  1.3  27/08/2013 T Jelliffe         Use only records with same EFFT_D
  #  1.4  21/10/2013 T Jelliffe         Insert/Delete changed records
  #  1.5  01/11/2013 T Jelliffe         Bug fixes - Row_Number not Rank
  #----------------------------------------------------------------------------
  # This info is for CBM use only
  # $LastChangedBy: jelifft $
  # $LastChangedDate: 2013-11-01 14:10:34 +1100 (Fri, 01 Nov 2013) $
  # $LastChangedRevision: 12954 $
  #
  #<================================================>--
  #< STEP 4 Delete all the original overlap records >--
  #<================================================>--
  exec("""
    !!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 30 OF FILE: /Users/psundaram/Documents/prjs/cba/agentic/cba-terradata-cdao-poc/current_state/prtf_tech_int_psst.bteq ***/!!!
    Delete A
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '32' COLUMN '1' OF THE SOURCE CODE STARTING AT 'From'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'From' ON LINE '32' COLUMN '1'. FAILED TOKEN WAS '%' ON LINE '33' COLUMN '3'. **
--From
--	 %%STARDATADB%%.DERV_PRTF_INT_PSST A
--	,%%VTECH%%.DERV_PRTF_INT_HIST_PSST B
	 Where
      A.INT_GRUP_I = B.INT_GRUP_I
      And PUBLIC.PERIOD_OVERLAPS_UDF(ARRAY_CONSTRUCT((A.JOIN_FROM_D, A.JOIN_TO_D), (B.JOIN_FROM_D, B.JOIN_TO_D))) !!!RESOLVE EWI!!! /*** SSC-EWI-TD0053 - SNOWFLAKE DOES NOT SUPPORT THE PERIOD DATATYPE, ALL PERIODS ARE HANDLED AS VARCHAR INSTEAD ***/!!!
	 """)

  if snowconvert.helpers.error_code != 0:
	 EXITERR()
	 return
  #<===============================================>--
  #< STEP 5 - Insert all deduped records into base >--
  #<===============================================>--
  exec("""

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '45' COLUMN '0' OF THE SOURCE CODE STARTING AT 'Insert'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'into' ON LINE '45' COLUMN '7'. **
--	 --<===============================================>--
----< STEP 5 - Insert all deduped records into base >--
----<===============================================>--

--Insert into %%STARDATADB%%.DERV_PRTF_INT_PSST
--Select
--   A.INT_GRUP_I
--  ,A.INT_GRUP_TYPE_C
--  ,A.JOIN_FROM_D
--  ,A.JOIN_TO_D
--  ,A.EFFT_D
--  ,A.EXPY_D
--  ,A.PTCL_N
--  ,A.REL_MNGE_I
--  ,A.VALD_FROM_D
--  ,A.VALD_TO_D
--  ,0 as PROS_KEY_I
--From
--  %%VTECH%%.DERV_PRTF_INT_HIST_PSST A
  """)

  if snowconvert.helpers.error_code != 0:
  EXITERR()
  return
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '63' COLUMN '1' OF THE SOURCE CODE STARTING AT 'Collect'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'Collect' ON LINE '63' COLUMN '1'. **
  #
  #--Collect Statistics on %%STARDATADB%%.DERV_PRTF_INT_PSST
   

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