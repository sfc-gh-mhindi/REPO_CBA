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
  # Object Name             :  DERV_ACCT_PATY_99_DROP_WORK_TABL.sql
  # Object Type             :  BTEQ
  #                           
  # Description             :  Drop work/staging tables that were used in this stream  
  #                          
  #----------------------------------------------------------------------------
  # Modification History
  # Date               Author           Version     Version Description
  # 04/06/2013         Helen Zak        1.0         Initial Version
  # 08/08/2013         Helen Zak        1.1         C0714578  - post-implementation fixes
  #                                                 drop new staging tables 
  # 21/08/2013         Helen Zak        1.2         C0726912 -post-implementation fix
  #                                                 Remove redundant tables 
  # 02/09/2013         Helen Zak        1.3         C0737261-post-implementation fix
  #                                                 Drop new staging tables 
  # 11/09/2013         Helen Zak        1.4         C0746488-post-implementation fix
  #                                                 Drop new staging tables 
  #----------------------------------------------------------------------------
  # Drop all work tables if we are in PROD
  exec("""
    SELECT
      1
    FROM
      (
        SELECT
          CASE
            WHEN UPPER(RTRIM('%%ENV_C%%')) = UPPER(RTRIM('PROD'))
              THEN 1
            ELSE 0
          END AS ENV
      ) T1
    WHERE
      T1.ENV = 1
    """)

  if snowconvert.helpers.error_code != 0:
    EXITERR()
    return

  # if no row returned - we are in DEV. Do not drop the tables. 
  # Complete the process successfully.
  if snowconvert.helpers.activity_count == 0:
    EXITOK()
    return
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '44' COLUMN '5' OF THE SOURCE CODE STARTING AT 'TABLE'. EXPECTED 'Drop Table' GRAMMAR. LAST MATCHING TOKEN WAS 'TABLE' ON LINE '44' COLUMN '5'. FAILED TOKEN WAS '%' ON LINE '44' COLUMN '11'. **
--           TABLE %%DDSTG%%.ACCT_PATY_REL_THA
    """)

  if snowconvert.helpers.error_code == 3807:
    NEXT1()
    return

  if snowconvert.helpers.error_code == 0:
    NEXT1()
    return
  snowconvert.helpers.quit_application(snowconvert.helpers.error_code)
  NEXT1()
  snowconvert.helpers.quit_application()
def NEXT1():
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '52' COLUMN '5' OF THE SOURCE CODE STARTING AT 'TABLE'. EXPECTED 'Drop Table' GRAMMAR. LAST MATCHING TOKEN WAS 'TABLE' ON LINE '52' COLUMN '5'. FAILED TOKEN WAS '%' ON LINE '52' COLUMN '11'. **
--           TABLE %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE
    """)

  if snowconvert.helpers.error_code == 3807:
    NEXT2()
    return

  if snowconvert.helpers.error_code == 0:
    NEXT2()
    return
  snowconvert.helpers.quit_application(snowconvert.helpers.error_code)
  NEXT2()
def NEXT2():
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '62' COLUMN '5' OF THE SOURCE CODE STARTING AT 'TABLE'. EXPECTED 'Drop Table' GRAMMAR. LAST MATCHING TOKEN WAS 'TABLE' ON LINE '62' COLUMN '5'. FAILED TOKEN WAS '%' ON LINE '62' COLUMN '11'. **
--           TABLE %%DDSTG%%.DERV_ACCT_PATY_CURR
    """)

  if snowconvert.helpers.error_code == 3807:
    NEXT3()
    return

  if snowconvert.helpers.error_code == 0:
    NEXT3()
    return
  snowconvert.helpers.quit_application(snowconvert.helpers.error_code)
  NEXT3()
def NEXT3():
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '70' COLUMN '5' OF THE SOURCE CODE STARTING AT 'TABLE'. EXPECTED 'Drop Table' GRAMMAR. LAST MATCHING TOKEN WAS 'TABLE' ON LINE '70' COLUMN '5'. FAILED TOKEN WAS '%' ON LINE '70' COLUMN '11'. **
--           TABLE %%DDSTG%%.DERV_PRTF_ACCT_STAG
    """)

  if snowconvert.helpers.error_code == 3807:
    NEXT4()
    return

  if snowconvert.helpers.error_code == 0:
    NEXT4()
    return
  snowconvert.helpers.quit_application(snowconvert.helpers.error_code)
  NEXT4()
def NEXT4():
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '77' COLUMN '5' OF THE SOURCE CODE STARTING AT 'TABLE'. EXPECTED 'Drop Table' GRAMMAR. LAST MATCHING TOKEN WAS 'TABLE' ON LINE '77' COLUMN '5'. FAILED TOKEN WAS '%' ON LINE '77' COLUMN '11'. **
--           TABLE %%DDSTG%%.DERV_PRTF_PATY_STAG
    """)

  if snowconvert.helpers.error_code == 3807:
    NEXT5()
    return

  if snowconvert.helpers.error_code == 0:
    NEXT5()
    return
  snowconvert.helpers.quit_application(snowconvert.helpers.error_code)
  NEXT5()
def NEXT5():
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '84' COLUMN '5' OF THE SOURCE CODE STARTING AT 'TABLE'. EXPECTED 'Drop Table' GRAMMAR. LAST MATCHING TOKEN WAS 'TABLE' ON LINE '84' COLUMN '5'. FAILED TOKEN WAS '%' ON LINE '84' COLUMN '11'. **
--           TABLE %%DDSTG%%.DERV_PRTF_ACCT_PATY_STAG
    """)

  if snowconvert.helpers.error_code == 3807:
    NEXT6()
    return

  if snowconvert.helpers.error_code == 0:
    NEXT6()
    return
  snowconvert.helpers.quit_application(snowconvert.helpers.error_code)
  NEXT6()
def NEXT6():
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '92' COLUMN '6' OF THE SOURCE CODE STARTING AT 'TABLE'. EXPECTED 'Drop Table' GRAMMAR. LAST MATCHING TOKEN WAS 'TABLE' ON LINE '92' COLUMN '6'. FAILED TOKEN WAS '%' ON LINE '92' COLUMN '12'. **
--            TABLE %%DDSTG%%.DERV_ACCT_PATY_RM
    """)

  if snowconvert.helpers.error_code == 3807:
    NEXT7()
    return

  if snowconvert.helpers.error_code == 0:
    NEXT7()
    return
  snowconvert.helpers.quit_application(snowconvert.helpers.error_code)
  NEXT7()
def NEXT7():
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '101' COLUMN '5' OF THE SOURCE CODE STARTING AT 'TABLE'. EXPECTED 'Drop Table' GRAMMAR. LAST MATCHING TOKEN WAS 'TABLE' ON LINE '101' COLUMN '5'. FAILED TOKEN WAS '%' ON LINE '101' COLUMN '11'. **
--           TABLE %%DDSTG%%.DERV_ACCT_PATY_ADD
    """)

  if snowconvert.helpers.error_code == 3807:
    NEXT8()
    return

  if snowconvert.helpers.error_code == 0:
    NEXT8()
    return
  snowconvert.helpers.quit_application(snowconvert.helpers.error_code)
  NEXT8()
def NEXT8():
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '108' COLUMN '5' OF THE SOURCE CODE STARTING AT 'TABLE'. EXPECTED 'Drop Table' GRAMMAR. LAST MATCHING TOKEN WAS 'TABLE' ON LINE '108' COLUMN '5'. FAILED TOKEN WAS '%' ON LINE '108' COLUMN '11'. **
--           TABLE %%DDSTG%%.DERV_ACCT_PATY_CHG
    """)

  if snowconvert.helpers.error_code == 3807:
    NEXT9()
    return

  if snowconvert.helpers.error_code == 0:
    NEXT9()
    return
  snowconvert.helpers.quit_application(snowconvert.helpers.error_code)
  NEXT9()
def NEXT9():
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '115' COLUMN '5' OF THE SOURCE CODE STARTING AT 'TABLE'. EXPECTED 'Drop Table' GRAMMAR. LAST MATCHING TOKEN WAS 'TABLE' ON LINE '115' COLUMN '5'. FAILED TOKEN WAS '%' ON LINE '115' COLUMN '11'. **
--           TABLE %%DDSTG%%.DERV_ACCT_PATY_FLAG
    """)

  if snowconvert.helpers.error_code == 3807:
    NEXT10()
    return

  if snowconvert.helpers.error_code == 0:
    NEXT10()
    return
  snowconvert.helpers.quit_application(snowconvert.helpers.error_code)
  NEXT10()
def NEXT10():
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '123' COLUMN '5' OF THE SOURCE CODE STARTING AT 'TABLE'. EXPECTED 'Drop Table' GRAMMAR. LAST MATCHING TOKEN WAS 'TABLE' ON LINE '123' COLUMN '5'. FAILED TOKEN WAS '%' ON LINE '123' COLUMN '11'. **
--           TABLE %%DDSTG%%.ACCT_PATY_REL_WSS
    """)

  if snowconvert.helpers.error_code == 3807:
    NEXT11()
    return

  if snowconvert.helpers.error_code == 0:
    NEXT11()
    return
  snowconvert.helpers.quit_application(snowconvert.helpers.error_code)
  NEXT11()
def NEXT11():
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '131' COLUMN '5' OF THE SOURCE CODE STARTING AT 'TABLE'. EXPECTED 'Drop Table' GRAMMAR. LAST MATCHING TOKEN WAS 'TABLE' ON LINE '131' COLUMN '5'. FAILED TOKEN WAS '%' ON LINE '131' COLUMN '11'. **
--           TABLE %%DDSTG%%.ACCT_REL_WSS_DITPS
    """)

  if snowconvert.helpers.error_code == 3807:
    NEXT12()
    return

  if snowconvert.helpers.error_code == 0:
    NEXT12()
    return
  snowconvert.helpers.quit_application(snowconvert.helpers.error_code)
  NEXT12()
def NEXT12():
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '141' COLUMN '5' OF THE SOURCE CODE STARTING AT 'TABLE'. EXPECTED 'Drop Table' GRAMMAR. LAST MATCHING TOKEN WAS 'TABLE' ON LINE '141' COLUMN '5'. FAILED TOKEN WAS '%' ON LINE '141' COLUMN '11'. **
--           TABLE %%DDSTG%%.GRD_GNRC_MAP_DERV_PATY_HOLD
    """)

  if snowconvert.helpers.error_code == 3807:
    NEXT13()
    return

  if snowconvert.helpers.error_code == 0:
    NEXT13()
    return
  snowconvert.helpers.quit_application(snowconvert.helpers.error_code)
  NEXT13()
def NEXT13():
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '148' COLUMN '5' OF THE SOURCE CODE STARTING AT 'TABLE'. EXPECTED 'Drop Table' GRAMMAR. LAST MATCHING TOKEN WAS 'TABLE' ON LINE '148' COLUMN '5'. FAILED TOKEN WAS '%' ON LINE '148' COLUMN '11'. **
--           TABLE %%DDSTG%%.GRD_GNRC_MAP_DERV_UNID_PATY
    """)

  if snowconvert.helpers.error_code == 3807:
    NEXT14()
    return

  if snowconvert.helpers.error_code == 0:
    NEXT14()
    return
  snowconvert.helpers.quit_application(snowconvert.helpers.error_code)
  NEXT14()
def NEXT14():
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '155' COLUMN '5' OF THE SOURCE CODE STARTING AT 'TABLE'. EXPECTED 'Drop Table' GRAMMAR. LAST MATCHING TOKEN WAS 'TABLE' ON LINE '155' COLUMN '5'. FAILED TOKEN WAS '%' ON LINE '155' COLUMN '11'. **
--           TABLE %%DDSTG%%.GRD_GNRC_MAP_DERV_PATY_REL
    """)

  if snowconvert.helpers.error_code == 3807:
    NEXT15()
    return

  if snowconvert.helpers.error_code == 0:
    NEXT15()
    return
  snowconvert.helpers.quit_application(snowconvert.helpers.error_code)
  NEXT15()
def NEXT15():
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '162' COLUMN '5' OF THE SOURCE CODE STARTING AT 'TABLE'. EXPECTED 'Drop Table' GRAMMAR. LAST MATCHING TOKEN WAS 'TABLE' ON LINE '162' COLUMN '5'. FAILED TOKEN WAS '%' ON LINE '162' COLUMN '11'. **
--           TABLE %%DDSTG%%.DERV_ACCT_PATY_DEL
    """)

  if snowconvert.helpers.error_code == 3807:
    NEXT16()
    return

  if snowconvert.helpers.error_code == 0:
    NEXT16()
    return
  snowconvert.helpers.quit_application(snowconvert.helpers.error_code)
  NEXT16()
def NEXT16():
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '170' COLUMN '5' OF THE SOURCE CODE STARTING AT 'TABLE'. EXPECTED 'Drop Table' GRAMMAR. LAST MATCHING TOKEN WAS 'TABLE' ON LINE '170' COLUMN '5'. FAILED TOKEN WAS '%' ON LINE '170' COLUMN '11'. **
--           TABLE %%DDSTG%%.ACCT_PATY_DEDUP
    """)

  if snowconvert.helpers.error_code == 3807:
    NEXT17()
    return

  if snowconvert.helpers.error_code == 0:
    NEXT17()
    return
  snowconvert.helpers.quit_application(snowconvert.helpers.error_code)
  NEXT17()
def NEXT17():
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '178' COLUMN '5' OF THE SOURCE CODE STARTING AT 'TABLE'. EXPECTED 'Drop Table' GRAMMAR. LAST MATCHING TOKEN WAS 'TABLE' ON LINE '178' COLUMN '5'. FAILED TOKEN WAS '%' ON LINE '178' COLUMN '11'. **
--           TABLE %%DDSTG%%.DERV_PRTF_ACCT_PATY_PSST
    """)

  if snowconvert.helpers.error_code == 3807:
    NEXT18()
    return

  if snowconvert.helpers.error_code == 0:
    NEXT18()
    return
  snowconvert.helpers.quit_application(snowconvert.helpers.error_code)
  NEXT18()
def NEXT18():
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '186' COLUMN '6' OF THE SOURCE CODE STARTING AT 'TABLE'. EXPECTED 'Drop Table' GRAMMAR. LAST MATCHING TOKEN WAS 'TABLE' ON LINE '186' COLUMN '6'. FAILED TOKEN WAS '%' ON LINE '186' COLUMN '12'. **
--            TABLE %%DDSTG%%.DERV_ACCT_PATY_NON_RM
    """)

  if snowconvert.helpers.error_code == 3807:
    NEXT19()
    return

  if snowconvert.helpers.error_code == 0:
    NEXT19()
    return
  snowconvert.helpers.quit_application(snowconvert.helpers.error_code)
  NEXT19()
def NEXT19():
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '193' COLUMN '6' OF THE SOURCE CODE STARTING AT 'TABLE'. EXPECTED 'Drop Table' GRAMMAR. LAST MATCHING TOKEN WAS 'TABLE' ON LINE '193' COLUMN '6'. FAILED TOKEN WAS '%' ON LINE '193' COLUMN '12'. **
--            TABLE %%DDSTG%%.DERV_ACCT_PATY_ROW_SECU_FIX
    """)

  if snowconvert.helpers.error_code == 3807:
    NEXT20()
    return

  if snowconvert.helpers.error_code == 0:
    NEXT20()
    return
  snowconvert.helpers.quit_application(snowconvert.helpers.error_code)
  NEXT20()
def NEXT20():
  snowconvert.helpers.quit_application()
  EXITOK()
def EXITOK():
  snowconvert.helpers.quit_application(0)
  EXITERR()
# $LastChangedBy: zakhe $
# $LastChangedDate: 2013-09-11 15:36:32 +1000 (Wed, 11 Sep 2013) $
# $LastChangedRevision: 12624 $
def EXITERR():
  snowconvert.helpers.quit_application(4)

if __name__ == "__main__":
  main()