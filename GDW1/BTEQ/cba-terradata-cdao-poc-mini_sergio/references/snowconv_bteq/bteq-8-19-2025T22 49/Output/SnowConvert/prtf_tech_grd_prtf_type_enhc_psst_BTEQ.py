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
  #----------------------------------------------------------------------------
  #
  #
  #  Ver  Date       Modified By            Description
  #  ---- ---------- ---------------------- -----------------------------------
  #  1.0  15/07/2013 T Jelliffe             Initial Version
  #  1.5  01/11/2013 T Jelliffe             Add the HIST persisted table
  #----------------------------------------------------------------------------
  # PDGRD DATA
  exec("""
    !!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 20 OF FILE: /Users/psundaram/Documents/prjs/cba/agentic/cba-terradata-cdao-poc/current_state/prtf_tech_grd_prtf_type_enhc_psst.bteq ***/!!!
    Delete
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '20' COLUMN '7' OF THE SOURCE CODE STARTING AT 'from'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'from' ON LINE '20' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '20' COLUMN '12'. **
--           from %%DGRDDB%%.GRD_PRTF_TYPE_ENHC_PSST All
    """)

  if snowconvert.helpers.error_code != 0:
    EXITERR()
    return
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '24' COLUMN '1' OF THE SOURCE CODE STARTING AT 'Collect'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'Collect' ON LINE '24' COLUMN '1'. **
  #
  #--Collect Statistics on %%DGRDDB%%.GRD_PRTF_TYPE_ENHC_PSST
   

  if snowconvert.helpers.error_code != 0:
    EXITERR()
    return
  exec("""

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '27' COLUMN '0' OF THE SOURCE CODE STARTING AT 'Insert'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'into' ON LINE '27' COLUMN '7'. **
--Insert into %%DGRDDB%%.GRD_PRTF_TYPE_ENHC_PSST
--Select
--   GP.PERD_D
--  ,GP.PRTF_TYPE_C
--  ,GP.PRTF_TYPE_M
--  ,GP.PRTF_CLAS_C
--  ,GP.PRTF_CLAS_M
--  ,GP.PRTF_CATG_C
--  ,GP.PRTF_CATG_M
--From
--  %%VTECH%%.GRD_PRTF_TYPE_ENHC GP
  """)

  if snowconvert.helpers.error_code != 0:
  EXITERR()
  return
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '41' COLUMN '1' OF THE SOURCE CODE STARTING AT 'Collect'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'Collect' ON LINE '41' COLUMN '1'. **
  #
  #--Collect Statistics on %%DGRDDB%%.GRD_PRTF_TYPE_ENHC_PSST
   

  if snowconvert.helpers.error_code != 0:
  EXITERR()
  return
  #< Populate the HISTORY version of the table >--
  exec("""
  !!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 47 OF FILE: /Users/psundaram/Documents/prjs/cba/agentic/cba-terradata-cdao-poc/current_state/prtf_tech_grd_prtf_type_enhc_psst.bteq ***/!!!
  Delete
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '47' COLUMN '7' OF THE SOURCE CODE STARTING AT 'from'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'from' ON LINE '47' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '47' COLUMN '12'. **
--         from %%DGRDDB%%.GRD_PRTF_TYPE_ENHC_HIST_PSST
  """)

  if snowconvert.helpers.error_code != 0:
  EXITERR()
  return
  exec("""

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '51' COLUMN '0' OF THE SOURCE CODE STARTING AT 'Insert'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'into' ON LINE '51' COLUMN '7'. **
--Insert into %%DGRDDB%%.GRD_PRTF_TYPE_ENHC_HIST_PSST
--Select
--   G.PRTF_TYPE_C
--  ,G.PRTF_TYPE_M
--  ,G.PRTF_CLAS_C
--  ,G.PRTF_CLAS_M
--  ,G.PRTF_CATG_C
--  ,G.PRTF_CATG_M
--	,MIN(PERD_D) as VALD_FROM_D
--  ,MAX(PERD_D) as VALD_TO_D
--From
--  %%VTECH%%.GRD_PRTF_TYPE_ENHC_PSST G
--Group By 1,2,3,4,5,6
  """)

  if snowconvert.helpers.error_code != 0:
  EXITERR()
  return
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '67' COLUMN '1' OF THE SOURCE CODE STARTING AT 'Collect'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'Collect' ON LINE '67' COLUMN '1'. **
  #
  #--Collect Statistics on %%DGRDDB%%.GRD_PRTF_TYPE_ENHC_HIST_PSST
   

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