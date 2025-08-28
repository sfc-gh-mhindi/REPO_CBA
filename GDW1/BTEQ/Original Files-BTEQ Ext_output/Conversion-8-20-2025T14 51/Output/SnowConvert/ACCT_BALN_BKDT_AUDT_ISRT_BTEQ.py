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
  # $LastChangedDate: 2012-02-28 09:08:37 +1100 (Tue, 28 Feb 2012) $
  # $LastChangedRevision: 9218 $
  #--------------------------------------------------------------------
  #----------------------------------------------------------------------------
  #
  #  Description :Populate the AUDT table for future reference as the records 
  #are going to be deleted from ACCT BALN BKDT and this acts as a driver for the 
  #ADJ RULE view 
  #
  #   Ver  Date       Modified By            Description
  #  ---- ---------- ---------------------- -----------------------------------
  #  1.0  2011-10-05 Suresh Vajapeyajula               Initial Version
  #----------------------------------------------------------------------------
  #Loading the records from ACCT_BALN_BKDT_STG1 to ACCT_BALN_BKDT_AUDT.
  #This table holds data that is going to be deleted from ACCT_BALN_BKDT. 
  #The pros keys in this table can be used to establish logical relationship between tables in the event 
  #of any failures and rollback. However, the Pros Keys from ACCT_BALN_ADJ  loaded as 
  #ADJ_PROS_KEY_EFFT_I could not be used to backtrack Adjustments for bulk loads or for 
  #multiple adjustments for same ACCT_I and overlapping preiods. A known limitation
  #
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '31' COLUMN '0' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '31' COLUMN '7'. **
------------------------------------------------------------------------
---- $LastChangedBy: vajapes $
---- $LastChangedDate: 2012-02-28 09:08:37 +1100 (Tue, 28 Feb 2012) $
---- $LastChangedRevision: 9218 $
------------------------------------------------------------------------
--------------------------------------------------------------------------------
----
----  Description :Populate the AUDT table for future reference as the records 
----are going to be deleted from ACCT BALN BKDT and this acts as a driver for the 
----ADJ RULE view 
----
----   Ver  Date       Modified By            Description
----  ---- ---------- ---------------------- -----------------------------------
----  1.0  2011-10-05 Suresh Vajapeyajula               Initial Version
--------------------------------------------------------------------------------

--/*Loading the records from ACCT_BALN_BKDT_STG1 to ACCT_BALN_BKDT_AUDT.
--This table holds data that is going to be deleted from ACCT_BALN_BKDT. 
--The pros keys in this table can be used to establish logical relationship between tables in the event 
--of any failures and rollback. However, the Pros Keys from ACCT_BALN_ADJ  loaded as 
--ADJ_PROS_KEY_EFFT_I could not be used to backtrack Adjustments for bulk loads or for 
--multiple adjustments for same ACCT_I and overlapping preiods. A known limitation
--*/
--INSERT INTO  %%CAD_PROD_DATA%%.ACCT_BALN_BKDT_AUDT
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
--ABAL_PROS_KEY_EFFT_I,
--ABAL_PROS_KEY_EXPY_I,
--ABAL_BKDT_PROS_KEY_I,
--ADJ_PROS_KEY_EFFT_I
--)
--SELECT
--STG1.ACCT_I,
--STG1.BALN_TYPE_C,
--STG1.CALC_FUNC_C,
--STG1.TIME_PERD_C,
--STG1.BALN_A,
--STG1.CALC_F,
--STG1.SRCE_SYST_C,
--STG1.ORIG_SRCE_SYST_C,
--STG1.LOAD_D,
--STG1.BKDT_EFFT_D,
--STG1.BKDT_EXPY_D,
--PKEY.PROS_KEY_EFFT_I,
--STG1.PROS_KEY_EFFT_I AS ABAL_PROS_KEY_EFFT_I,
--STG1.PROS_KEY_EXPY_I AS ABAL_PROS_KEY_EXPY_I,
--STG2.BKDT_PROS_KEY_I AS ABAL_BKDT_PROS_KEY_I,
--ADJ.PROS_KEY_EFFT_I AS ADJ_PROS_KEY_EFFT_I
--FROM
--%%DDSTG%%.ACCT_BALN_BKDT_STG1 STG1
--INNER JOIN
--/*Capturing the maximum pros_key_efft_i in the event of multiple pros keys for one account 
--and populating for Auditing purposes*/
--(SELECT ACCT_I, MAX(PROS_KEY_EFFT_I) AS PROS_KEY_EFFT_I FROM
--%%DDSTG%%.ACCT_BALN_BKDT_ADJ_RULE
--GROUP BY 1
--)ADJ
--ON
--STG1.ACCT_I = ADJ.ACCT_I
--CROSS JOIN
--/*Capture tha latest pros key [from the parent process] and update the audt table*/
--(SELECT MAX(BKDT_PROS_KEY_I) AS BKDT_PROS_KEY_I
--FROM %%DDSTG%%.ACCT_BALN_BKDT_STG2)STG2
--CROSS JOIN
--/*Capture tha latest pros key [from the Auditing process] and update the audt table*/
--(SELECT MAX(PROS_KEY_I)  AS PROS_KEY_EFFT_I
--FROM %%VTECH%%.UTIL_PROS_ISAC WHERE
--CONV_M='CAD_X01_ACCT_BALN_BKDT_AUDT')PKEY
""")

  if snowconvert.helpers.error_code != 0:
EXITERR()
return
  #Update UTIL_PROS_ISAC with the  flags , completed timestamp and record counts inserted
  exec("""

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '93' COLUMN '0' OF THE SOURCE CODE STARTING AT 'UPDATE'. EXPECTED 'Update' GRAMMAR. LAST MATCHING TOKEN WAS 'UPDATE' ON LINE '93' COLUMN '0'. FAILED TOKEN WAS '%' ON LINE '93' COLUMN '7'. **
--/*Update UTIL_PROS_ISAC with the  flags , completed timestamp and record counts inserted*/
--UPDATE %%CAD_PROD_DATA%%.UTIL_PROS_ISAC
--FROM
--(SELECT COUNT(*) FROM
--%%DDSTG%%.ACCT_BALN_BKDT_STG1)A(INS_CNT)
--SET
--        COMT_F = 'Y',
--	SUCC_F='Y',
--	COMT_S =  CURRENT_TIMESTAMP(0),
--	SYST_INS_Q = A.INS_CNT
-- WHERE PROS_KEY_I = (SELECT MAX(PROS_KEY_I) FROM %%VTECH%%.UTIL_PROS_ISAC
-- WHERE CONV_M='CAD_X01_ACCT_BALN_BKDT_AUDT')
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