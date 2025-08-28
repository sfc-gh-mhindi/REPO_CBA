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

  if snowconvert.helpers.error_level != 0:
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
  # Object Name             :  DERV_ACCT_PATY_07_CRAT_DELTAS.sql
  # Object Type             :  BTEQ
  #                           
  # Description             :  determine what changed in the table and apply the changes
  #----------------------------------------------------------------------------
  # Modification History
  # Date               Author           Version     Version Description
  # 04/06/2013         Helen Zak        1.0         Initial Version
  # 18/08/2013         Helen Zak        1.1         C0726912 - post-implementation fix
  #                                                 Correct delta processing  - check for the rows
  #                                                 that were effective before but not in the _FLAG
  #                                                 table now  
  # 03/09/2013         Helen Zak        1.2         C0730261
  #                                                 table DERV_ACCT_PATY_FLAG now contains current
  #                                                 rows of interest (so that the original 
  #                                                 table DERV)ACCT_PATY_CURR is preserved).
  #                                                 This is done so that it is easier to rerun
  #----------------------------------------------------------------------------
  #1. Insert rows into PDDSTG.DERV_ACCT_PATY_ADD that exist now but didn't exist before 
  exec("""
    !!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 36 OF FILE: /Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/Original Files-BTEQ Ext/DERV_ACCT_PATY_07_CRAT_DELTAS.bteq ***/!!!
    DELETE
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '36' COLUMN '7' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '36' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '36' COLUMN '12'. **
--           FROM %%DDSTG%%.DERV_ACCT_PATY_ADD
    """)

  if snowconvert.helpers.error_code != 0:
    EXITERR()
    return
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '40' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '40' COLUMN '2'. **
  #--IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
   
  using = snowconvert.helpers.using("EXTR_D", "VARCHAR(10)", rows_to_read = 1)
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '43' COLUMN '1' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '43' COLUMN '8'. **
--INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_ADD
--SELECT T1.ACCT_I
--      ,T1.PATY_I
--      ,T1.ASSC_ACCT_I
--      ,T1.PATY_ACCT_REL_C
--      ,T1.PRFR_PATY_F
--      ,T1.SRCE_SYST_C
--      ,T1.EFFT_D
--      ,T1.EXPY_D
--      ,T1.ROW_SECU_ACCS_C
--FROM %%DDSTG%%.DERV_ACCT_PATY_FLAG T1
--LEFT JOIN %%VTECH%%.DERV_ACCT_PATY T2

--   ON T1.ACCT_I = T2.ACCT_I
--  AND T1.PATY_I = T2.PATY_I
--  AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
--  AND :EXTR_D BETWEEN T2.EFFT_D AND T2.EXPY_D

--  WHERE :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D
--    AND T2.ACCT_I IS NULL
--  GROUP BY 1,2,3,4,5,6,7,8,9
  """, using = using)

  if snowconvert.helpers.error_code != 0:
  EXITERR()
  return
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '67' COLUMN '3' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '67' COLUMN '3'. **
  #
  #--  COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_ADD
   

  if snowconvert.helpers.error_code != 0:
  EXITERR()
  return
  # 2.Insert rows into PDDSTG.DERV_ACCT_PATY_CHG that changed 
  exec("""
  !!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 73 OF FILE: /Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/Original Files-BTEQ Ext/DERV_ACCT_PATY_07_CRAT_DELTAS.bteq ***/!!!
  DELETE
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '73' COLUMN '7' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '73' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '73' COLUMN '12'. **
--         FROM %%DDSTG%%.DERV_ACCT_PATY_CHG
  """)

  if snowconvert.helpers.error_code != 0:
  EXITERR()
  return
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '76' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '76' COLUMN '2'. **
  #--IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
   
  using = snowconvert.helpers.using("EXTR_D", "VARCHAR(10)", rows_to_read = 1)
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '79' COLUMN '1' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '79' COLUMN '8'. **
--INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CHG
--SELECT T1.ACCT_I
--      ,T1.PATY_I
--      ,T1.ASSC_ACCT_I
--      ,T1.PATY_ACCT_REL_C
--      ,T1.PRFR_PATY_F
--      ,T1.SRCE_SYST_C
--      ,T1.EFFT_D
--      ,T1.EXPY_D
--      ,T1.ROW_SECU_ACCS_C
--FROM %%DDSTG%%.DERV_ACCT_PATY_FLAG T1
--JOIN %%VTECH%%.DERV_ACCT_PATY T2

--  ON T1.ACCT_I = T2.ACCT_I
--  AND T1.PATY_I = T2.PATY_I
--  AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C

--  WHERE (T1.ASSC_ACCT_I <> T2.ASSC_ACCT_I
--  OR T1.PRFR_PATY_F <> T2.PRFR_PATY_F
--  OR T1.SRCE_SYST_C <> T2.SRCE_SYST_C
--  )
--  AND :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D
--  AND :EXTR_D BETWEEN T2.EFFT_D AND T2.EXPY_D
--   GROUP BY 1,2,3,4,5,6,7,8,9
  """, using = using)

  if snowconvert.helpers.error_code != 0:
  EXITERR()
  return
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '106' COLUMN '2' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '106' COLUMN '2'. **
  #
  #-- COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CHG
   

  if snowconvert.helpers.error_code != 0:
  EXITERR()
  return
  #3. insert rows into PDDSTG.DERV_ACCT_PATY_DEL that were effective before but not in the current table any more 
  exec("""
  !!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 112 OF FILE: /Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/Original Files-BTEQ Ext/DERV_ACCT_PATY_07_CRAT_DELTAS.bteq ***/!!!
  DELETE
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '112' COLUMN '7' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '112' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '112' COLUMN '12'. **
--         FROM %%DDSTG%%.DERV_ACCT_PATY_DEL
  """)

  if snowconvert.helpers.error_code != 0:
  EXITERR()
  return
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '115' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '115' COLUMN '2'. **
  #--IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
   
  using = snowconvert.helpers.using("EXTR_D", "VARCHAR(10)", rows_to_read = 1)
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '118' COLUMN '1' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '118' COLUMN '8'. **
--INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_DEL
--SELECT T1.ACCT_I
--      ,T1.PATY_I
--      ,T1.ASSC_ACCT_I
--      ,T1.PATY_ACCT_REL_C
--      ,T1.PRFR_PATY_F
--      ,T1.SRCE_SYST_C
--      ,T1.EFFT_D
--      ,T1.EXPY_D
--      ,T1.ROW_SECU_ACCS_C
--FROM %%VTECH%%.DERV_ACCT_PATY T1
--LEFT JOIN %%DDSTG%%.DERV_ACCT_PATY_FLAG T2

--  ON T1.ACCT_I = T2.ACCT_I
--  AND T1.PATY_I = T2.PATY_I
--  AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
--  AND :EXTR_D BETWEEN T2.EFFT_D AND T2.EXPY_D

--  WHERE :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D
--    AND T2.ACCT_I IS NULL
--  GROUP BY 1,2,3,4,5,6,7,8,9
  """, using = using)

  if snowconvert.helpers.error_code != 0:
  EXITERR()
  return
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '142' COLUMN '3' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '142' COLUMN '3'. **
  #
  #--  COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_DEL
   

  if snowconvert.helpers.error_code != 0:
  EXITERR()
  return
  snowconvert.helpers.quit_application(0)
  EXITERR()
  snowconvert.helpers.quit_application()
# $LastChangedBy: zakhe $
# $LastChangedDate: 2013-09-04 10:43:45 +1000 (Wed, 04 Sep 2013) $
# $LastChangedRevision: 12585 $
def EXITERR():
  snowconvert.helpers.quit_application(4)

if __name__ == "__main__":
  main()