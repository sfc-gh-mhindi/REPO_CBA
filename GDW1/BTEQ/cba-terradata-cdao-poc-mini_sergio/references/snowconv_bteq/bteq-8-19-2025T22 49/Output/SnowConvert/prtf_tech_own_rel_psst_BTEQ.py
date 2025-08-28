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
  # Object Name             :  prtf_tech_own_rel_psst.sql
  # Object Type             :  BTEQ
  #                           
  # Description             :  Persist rows for DERV_PRTF_OWN view
  #----------------------------------------------------------------------------
  #   Modification history
  #---------------------------------------------------------------------------- 
  #  Ver  Date           Modified By    Description
  #  ---- -----------   ------------    ---------------------------
  #  1.0  09/01/2014     Helen Zak     Initial Version
  #  2.0  07/02/2014     Zeewa Lwin    Remove VALD_FROM_D and VALD_TO_D
  #                                    values extracting from GRD table 
  #----------------------------------------------------------------------------
  # This info is for CBM use only
  # $LastChangedBy: lwinze $
  # $LastChangedDate: 2014-02-11 16:18:10 +1100 (Tue, 11 Feb 2014) $
  # $LastChangedRevision: 13212 $
  #
  exec("""
    !!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 31 OF FILE: /Users/psundaram/Documents/prjs/cba/agentic/cba-terradata-cdao-poc/current_state/prtf_tech_own_rel_psst.bteq ***/!!!
    Delete
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '31' COLUMN '7' OF THE SOURCE CODE STARTING AT 'from'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'from' ON LINE '31' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '31' COLUMN '12'. **
--           from %%STARDATADB%%.DERV_PRTF_OWN_REL All
    """)

  if snowconvert.helpers.error_code != 0:
    EXITERR()
    return
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '37' COLUMN '1' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '37' COLUMN '1'. **
  #
  #---- Collect stats after deletion so that the optimiser "knows" that the table is empty
  #
  #--COLLECT STATS  %%STARDATADB%%.DERV_PRTF_OWN_REL
   

  if snowconvert.helpers.error_code != 0:
    EXITERR()
    return
  exec("""

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '42' COLUMN '0' OF THE SOURCE CODE STARTING AT 'Insert'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'into' ON LINE '42' COLUMN '7'. **
--Insert into %%STARDATADB%%.DERV_PRTF_OWN_REL
--(
--    INT_GRUP_I
--  , DERV_PRTF_CATG_C
--  , DERV_PRTF_CLAS_C
--  , DERV_PRTF_TYPE_C
--  , VALD_FROM_D
--  , VALD_TO_D
--  , EFFT_D
--  , EXPY_D
--  , PTCL_N
--  , REL_MNGE_I
--  , PRTF_CODE_X
--  , DERV_PRTF_ROLE_C
--  , ROLE_PLAY_TYPE_X
--  , ROLE_PLAY_I
--  , SRCE_SYST_C
--  , ROW_SECU_ACCS_C
--)
--SELECT
--    DT1.INT_GRUP_I
--  , GPTE2.PRTF_CATG_C AS DERV_PRTF_CATG_C
--  , GPTE2.PRTF_CLAS_C AS DERV_PRTF_CLAS_C
--  , DT1.DERV_PRTF_TYPE_C AS DERV_PRTF_TYPE_C
--  , DT1.VALD_FROM_D
--  , DT1.VALD_TO_D
--  , DT1.EFFT_D AS EFFT_D
--  , DT1.EXPY_D AS EXPY_D
--  , DT1.PTCL_N AS PTCL_N
--  , DT1.REL_MNGE_I AS REL_MNGE_I
--  , DT1.PRTF_CODE_X AS PRTF_CODE_X
--  , DT1.DERV_PRTF_ROLE_C AS DERV_PRTF_ROLE_C
--  , DT1.ROLE_PLAY_TYPE_X AS ROLE_PLAY_TYPE_X
--  , DT1.ROLE_PLAY_I AS ROLE_PLAY_I
--  , DT1.SRCE_SYST_C AS SRCE_SYST_C
--  , DT1.ROW_SECU_ACCS_C AS ROW_SECU_ACCS_C
--FROM
--    ( SELECT
--          IG2.INT_GRUP_I AS INT_GRUP_I
--        , IGED.EFFT_D AS EFFT_D
--        , IGED.EXPY_D AS EXPY_D
--        , IG2.INT_GRUP_TYPE_C AS DERV_PRTF_TYPE_C
--        , CAST( IG2.PTCL_N AS SMALLINT ) AS PTCL_N
--        , IG2.REL_MNGE_I AS REL_MNGE_I
--        , ( CASE
--               WHEN ( IG2.PTCL_N IS NULL ) OR ( IG2.REL_MNGE_I IS NULL ) THEN 'NA'
--               ELSE TRIM ( IG2.PTCL_N ) || TRIM ( IG2.REL_MNGE_I )
--               END  ) AS PRTF_CODE_X
--        , IGED.DERV_PRTF_ROLE_C AS DERV_PRTF_ROLE_C
--        , IGED.ROLE_PLAY_TYPE_X AS ROLE_PLAY_TYPE_X
--        , IGED.ROLE_PLAY_I AS ROLE_PLAY_I
--        , IGED.SRCE_SYST_C AS SRCE_SYST_C
--        , IGED.ROW_SECU_ACCS_C AS ROW_SECU_ACCS_C
--        , ( CASE
--               WHEN IG2.JOIN_FROM_D > IGED.JOIN_FROM_D
--               THEN IG2.JOIN_FROM_D
--               ELSE IGED.JOIN_FROM_D
--               END  ) AS VALD_FROM_D
--        , ( CASE
--               WHEN IG2.JOIN_TO_D < IGED.JOIN_TO_D
--               THEN IG2.JOIN_TO_D
--               ELSE IGED.JOIN_TO_D
--               END  ) AS VALD_TO_D

--   FROM %%VTECH%%.DERV_PRTF_OWN_PSST IGED

--   INNER JOIN %%VTECH%%.DERV_PRTF_INT_PSST IG2
--           ON IGED.INT_GRUP_I = IG2.INT_GRUP_I
--          AND IGED.JOIN_TO_D >= IG2.JOIN_FROM_D
--          AND IGED.JOIN_FROM_D <= IG2.JOIN_TO_D
--) DT1

--INNER JOIN %%VTECH%%.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE2
--        ON GPTE2.PRTF_TYPE_C = DT1.DERV_PRTF_TYPE_C
""")

  if snowconvert.helpers.error_code != 0:
EXITERR()
return
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '122' COLUMN '1' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '122' COLUMN '1'. **
  #
  #--COLLECT STATS  %%STARDATADB%%.DERV_PRTF_OWN_REL
   

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