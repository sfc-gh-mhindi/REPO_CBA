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
  # $LastChangedDate: 2012-05-03 14:09:15 +1000 (Thu, 03 May 2012) $
  # $LastChangedRevision: 9596 $
  #--------------------------------------------------------------------
  #----------------------------------------------------------------------------
  #
  #  Description :Reconciliation process
  #
  #   Ver  Date       Modified By            Description
  #  ---- ---------- ---------------------- -----------------------------------
  #  1.0  2011-10-05 Suresh Vajapeyajula               Initial Version
  #----------------------------------------------------------------------------
  #Delete previous run data from ACCT_BALN_BKDT_RECN
  exec("""
    DELETE
    """)
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '23' COLUMN '7' OF THE SOURCE CODE STARTING AT '%'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS '120' ON LINE '7' COLUMN '12'. FAILED TOKEN WAS '%' ON LINE '23' COLUMN '7'. **
  #--%%CAD_PROD_DATA%%.ACCT_BALN_BKDT_RECN
   

  if snowconvert.helpers.error_code != 0:
    EXITERR()
    return
  #Insert data into ACCT_BALN_BKDT_RECN table
  exec("""

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '28' COLUMN '0' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '28' COLUMN '7'. **
--/*Insert data into ACCT_BALN_BKDT_RECN table*/
--INSERT INTO %%CAD_PROD_DATA%%.ACCT_BALN_BKDT_RECN
--SELECT
--DT.ACCT_I
--,BAL.EFFT_D
--,BAL.EXPY_D
--,DT.BALN_A
--,NULL AS PROS_KEY_EFFT_I
--FROM
--(
--SELECT
--B.ACCT_I,
--B.BALN_A
--FROM
--/*Qualifying only those records that are considered for applying adjustments as part of this run*/
--%%DDSTG%%.ACCT_BALN_BKDT_STG2 A
--INNER JOIN
--%%VTECH%%.ACCT_BALN B
--ON
--A.ACCT_I=B.ACCT_I
--WHERE
--B.BALN_TYPE_C='BALN'
--AND B.CALC_FUNC_C='SPOT'
--AND B.TIME_PERD_C = 'E'
--/*reconciling the balances only for the current record*/
--AND CURRENT_DATE BETWEEN B.EFFT_D AND B.EXPY_D

--MINUS

--SELECT
--STG.ACCT_I,
--(CASE WHEN STG.BKDT_EFFT_D > BKDT.BKDT_EFFT_D  THEN STG.BALN_A
--ELSE BKDT.BALN_A END ) AS BALN_A
--FROM
--(
--SELECT
--A.ACCT_I
--,A.BKDT_EFFT_D
--,A.BKDT_EXPY_D
--,A.BALN_A
--FROM
--%%DDSTG%%.ACCT_BALN_BKDT_STG2 A
--WHERE
--A.BALN_TYPE_C='BDCL'
--AND A.CALC_FUNC_C='SPOT'
--AND A.TIME_PERD_C = 'E'
--AND CURRENT_DATE BETWEEN A.BKDT_EFFT_D AND A.BKDT_EXPY_D
--)STG
--INNER JOIN
--(
--SELECT
--B.ACCT_I
--,B.BKDT_EFFT_D
--,B.BKDT_EXPY_D
--,B.BALN_A
--FROM
--%%DDSTG%%.ACCT_BALN_BKDT_STG2 A
--INNER JOIN
--%%VTECH%%.ACCT_BALN_BKDT B
--ON
--A.ACCT_I = B.ACCT_I
--WHERE
--B.BALN_TYPE_C='BDCL'
--AND B.CALC_FUNC_C='SPOT'
--AND B.TIME_PERD_C = 'E'
--AND CURRENT_DATE BETWEEN B.BKDT_EFFT_D AND B.BKDT_EXPY_D
--)BKDT
--ON
--STG.ACCT_I = BKDT.ACCT_I
--)DT
--INNER JOIN
--%%VTECH%%.ACCT_BALN BAL
--ON
--DT.ACCT_I = BAL.ACCT_I
--WHERE
--BAL.BALN_TYPE_C='BDCL'
--AND BAL.CALC_FUNC_C='SPOT'
--AND BAL.TIME_PERD_C = 'E'
--AND CURRENT_DATE BETWEEN BAL.EFFT_D AND BAL.EXPY_D
""")

  if snowconvert.helpers.activity_count != 0:
ERR_SEV()
return

  if snowconvert.helpers.error_code != 0:
EXITERR()
return
  exec("""


-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '114' COLUMN '0' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '114' COLUMN '7'. **
--INSERT INTO %%CAD_PROD_DATA%%.ACCT_BALN_BKDT_RECN
--SELECT
-- DT1.ACCT_I
--,STG.BKDT_EFFT_D
--,STG.BKDT_EXPY_D
--,STG.BALN_A
--,NULL AS PROS_KEY_EFFT_I
--FROM
--(SELECT
--STG.ACCT_I,
--(CASE WHEN STG.BKDT_EFFT_D > BKDT.BKDT_EFFT_D  THEN STG.BALN_A
--ELSE BKDT.BALN_A END ) AS BALN_A,
--NULL AS PROS_KEY_EFFT_I
--FROM
--(
--SELECT
--A.ACCT_I
--,A.BKDT_EFFT_D
--,A.BKDT_EXPY_D
--,A.BALN_A
--FROM
--%%DDSTG%%.ACCT_BALN_BKDT_STG2 A
--WHERE
--A.BALN_TYPE_C='BDCL'
--AND A.CALC_FUNC_C='SPOT'
--AND A.TIME_PERD_C = 'E'
--AND CURRENT_DATE BETWEEN A.BKDT_EFFT_D AND A.BKDT_EXPY_D
--)STG
--INNER JOIN
--(
--SELECT
--B.ACCT_I
--,B.BKDT_EFFT_D
--,B.BKDT_EXPY_D
--,B.BALN_A
--FROM
--%%DDSTG%%.ACCT_BALN_BKDT_STG2 A
--INNER JOIN
--%%VTECH%%.ACCT_BALN_BKDT B
--ON
--A.ACCT_I = B.ACCT_I
--WHERE
--B.BALN_TYPE_C='BDCL'
--AND B.CALC_FUNC_C='SPOT'
--AND B.TIME_PERD_C = 'E'
--AND CURRENT_DATE BETWEEN B.BKDT_EFFT_D AND B.BKDT_EXPY_D
--)BKDT
--ON
--STG.ACCT_I = BKDT.ACCT_I

--MINUS

--SELECT
--B.ACCT_I,
--B.BALN_A,
--NULL AS PROS_KEY_EFFT_I
--FROM
--/*Qualifying only those records that are considered for applying adjustments as part of this run*/
--%%DDSTG%%.ACCT_BALN_BKDT_STG2 A
--INNER JOIN
--%%VTECH%%.ACCT_BALN B
--ON
--A.ACCT_I=B.ACCT_I
--WHERE
--B.BALN_TYPE_C='BALN'
--AND B.CALC_FUNC_C='SPOT'
--AND B.TIME_PERD_C = 'E'
--/*reconciling the balances only for the current record*/
--AND CURRENT_DATE BETWEEN B.EFFT_D AND B.EXPY_D
--)DT1
--INNER JOIN
--%%DDSTG%%.ACCT_BALN_BKDT_STG2 STG
--ON
--DT1.ACCT_I=STG.ACCT_I
--AND CURRENT_DATE BETWEEN STG.BKDT_EFFT_D AND STG.BKDT_EXPY_D
""")

  #Activity Count > 0 implies - An adjustment affecting the balances of an open record for 
  #the ACCT_I loaded into ACCT_BALN_BKDT_RECN table.This impacts the next daily delta 
  #load of Datastage. Thereby it is highly recommended to fix the issue 
  #before you restart in such a failures
  if snowconvert.helpers.activity_count != 0:
ERR_SEV()
return

  if snowconvert.helpers.error_code != 0:
EXITERR()
return
  #Update the flags,counts for the process in UTIl_PROS_ISAC
  exec("""

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '202' COLUMN '0' OF THE SOURCE CODE STARTING AT 'UPDATE'. EXPECTED 'Update' GRAMMAR. LAST MATCHING TOKEN WAS 'UPDATE' ON LINE '202' COLUMN '0'. FAILED TOKEN WAS '%' ON LINE '202' COLUMN '7'. **
--/*Update the flags,counts for the process in UTIl_PROS_ISAC*/
--UPDATE %%CAD_PROD_DATA%%.UTIL_PROS_ISAC
--FROM
--(SELECT COUNT(*) FROM
--%%VTECH%%.ACCT_BALN_BKDT_RECN)A(INS_CNT)
--SET     COMT_F = 'Y',
--	SUCC_F='Y',
--	COMT_S =  CURRENT_TIMESTAMP(0),
--	SYST_INS_Q = A.INS_CNT
-- WHERE PROS_KEY_I = (SELECT MAX(PROS_KEY_I) FROM %%VTECH%%.UTIL_PROS_ISAC
-- WHERE CONV_M='CAD_X01_ACCT_BALN_BKDT_RECN')
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
  ERR_SEV()
def ERR_SEV():
  #Updating the records with the latest pros key 
  exec("""

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '228' COLUMN '0' OF THE SOURCE CODE STARTING AT 'UPDATE'. EXPECTED 'Update' GRAMMAR. LAST MATCHING TOKEN WAS 'UPDATE' ON LINE '228' COLUMN '0'. FAILED TOKEN WAS '%' ON LINE '228' COLUMN '7'. **
-- /*Updating the records with the latest pros key */
--UPDATE %%CAD_PROD_DATA%%.ACCT_BALN_BKDT_RECN
--FROM
--(SELECT MAX(PROS_KEY_I) AS PROS_KEY_I
--FROM %%VTECH%%.UTIL_PROS_ISAC
--WHERE CONV_M= 'CAD_X01_ACCT_BALN_BKDT_RECN') D (PROS_KEY_I)
--SET PROS_KEY_EFFT_I = D.PROS_KEY_I
""")
  snowconvert.helpers.quit_application(1)
  #** SSC-FDM-0027 - REMOVED NEXT STATEMENT, NOT APPLICABLE IN SNOWFLAKE. LOGOFF **
  #.LOGOFF
   
  snowconvert.helpers.quit_application()

if __name__ == "__main__":
  main()