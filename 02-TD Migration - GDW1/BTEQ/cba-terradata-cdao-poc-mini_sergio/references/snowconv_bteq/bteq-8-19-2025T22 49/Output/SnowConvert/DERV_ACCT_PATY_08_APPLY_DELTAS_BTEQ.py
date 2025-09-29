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
  # Object Name             :  DERV_ACCT_PATY_08_APPLY_DELTAS.sql
  # Object Type             :  BTEQ
  #                           
  # Description             :  Apply the changes as determined in previous step
  #----------------------------------------------------------------------------
  # Modification History
  # Date               Author           Version     Version Description
  # 04/06/2013         Helen Zak        1.0         Initial Version
  # 31/07/2013         Megan Disch      1.1         Pros key starts @ posn 4
  # 08/08/2013         Helen Zak        1.2         C0714578 - post-implementation fix
  #                                                 1) read correct file (remove %%TBSHORT%% as it's not used)
  #                                                 2) use PROS_KEY to read the row from UTIL_PROS_ISAC to obtain 
  #                                                    BTCH_RUN_D, write both into a file
  #                                                 3) import this file and use the values for applying deltas
  # 15/08/2013         Helen Zak        1.3         C0726912 - post-implementation fix
  #                                                 Correct setting EFFT_D and EXPY_D for adds and updates.
  #                                                 Logically delete (by updating EXPY_D) rows that are no longer effective 
  # 28/08/2013         Helen Zak        1.4         C0733426 
  #                                                 Use EFFT_D when applying deletions
  # 11/09/2013         Helen Zak      1.5           C0746488
  #                                                Syncronise values of ROW_SECU_ACCS_C in DERV_ACCT_PATY with the values
  #                                                for current rows in ACCT_PATY.              
  #----------------------------------------------------------------------------
  # Remove the existing pros key date file
  snowconvert.helpers.os("""rm /cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_PROS_KEY_DATE.txt""")
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '42' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '42' COLUMN '2'. **
  #--IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_PROS_KEY
   
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '42' COLUMN '77' OF THE SOURCE CODE STARTING AT 'txt'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS '.' ON LINE '42' COLUMN '76'. FAILED TOKEN WAS 'txt' ON LINE '42' COLUMN '77'. **
  #--txt
   
  #** SSC-EWI-TD0005 - THE STATEMENT WAS CONVERTED BUT ITS FUNCTIONALITY IS NOT IMPLEMENTED YET **
  Export.report(fr"/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_PROS_KEY_DATE.txt", ",")
  using = snowconvert.helpers.using("FILLER", "CHAR(4)", "PROSKEY", "CHAR(10)", rows_to_read = 1)
  exec("""
    SELECT
      RPAD(TO_VARCHAR(CAST(TRUNC(PROS_KEY_I) AS INTEGER) !!!RESOLVE EWI!!! /*** SSC-EWI-TD0041 - TRUNC FUNCTION WAS ADDED TO ENSURE INTEGER. MAY NEED CHANGES IF NOT NUMERIC OR STRING. ***/!!!, 'TM'), 10),
      RPAD(CAST(BTCH_RUN_D AS CHAR(10)), 10)

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '51' COLUMN '5' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '51' COLUMN '5'. FAILED TOKEN WAS '%' ON LINE '51' COLUMN '10'. **
--    FROM %%VTECH%%.UTIL_PROS_ISAC
    WHERE
      PROS_KEY_I = CAST(trim(:PROSKEY) as DECIMAL(10, 0))
    """, using = using)
  #** SSC-EWI-TD0005 - THE STATEMENT WAS CONVERTED BUT ITS FUNCTIONALITY IS NOT IMPLEMENTED YET **
  Export.reset()

  if snowconvert.helpers.error_code != 0:
    EXITERR()
    return
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '61' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '61' COLUMN '2'. **
  #--IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_PROS_KEY_DATE.txt
   
  using = snowconvert.helpers.using("FILLER", "CHAR(4)", "PROSKEY", "CHAR(10)", "EXTR_D", "CHAR(10)", rows_to_read = 1)
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '68' COLUMN '1' OF THE SOURCE CODE STARTING AT 'UPDATE'. EXPECTED 'Update' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '69' COLUMN '1'. **
--UPDATE T1
--FROM   %%STARDATADB%%.DERV_ACCT_PATY T1
--       ,%%DDSTG%%.DERV_ACCT_PATY_DEL T2
--    SET    EXPY_D = CAST(:EXTR_D AS DATE) -1 (FORMAT'YYYY-MM-DD')(CHAR(10))
--          ,PROS_KEY_EXPY_I = CAST(trim(:PROSKEY) as INTEGER)

--  WHERE T1.ACCT_I = T2.ACCT_I
--    AND T1.PATY_I = T2.PATY_I
--    AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
--    AND T1.EFFT_D = T2.EFFT_D
--    AND :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D
--    AND :EXTR_D BETWEEN T2.EFFT_D AND T2.EXPY_D
    """, using = using)
  exec("""

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '82' COLUMN '2' OF THE SOURCE CODE STARTING AT '.'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS ';' ON LINE '80' COLUMN '3'. FAILED TOKEN WAS '.' ON LINE '82' COLUMN '2'. **
-- .
    """, using = using)
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '82' COLUMN '3' OF THE SOURCE CODE STARTING AT 'IF'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '61' COLUMN '2'. FAILED TOKEN WAS 'IF' ON LINE '82' COLUMN '3'. **
  #--IF ERRORCODE   <> 0 THEN .GOTO EXITERR
  #
  #----2. expire current rows that changed
  #
  #--.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_PROS_KEY_DATE.txt
  #--USING
  #
  #--(  FILLER CHAR(4)
  #--   ,PROSKEY CHAR(10)
  #--   ,EXTR_D CHAR(10)
  #--   )
  #--UPDATE T1
  #--FROM %%STARDATADB%%.DERV_ACCT_PATY T1,
  #--      (sel * from  PDDSTG.DERV_ACCT_PATY_CHG  qualify row_number() over (partition by ACCT_I,PATY_I,PATY_ACCT_REL_C order by efft_d desc)=1)T2
  #
  #--SET EXPY_D = CAST(:EXTR_D AS DATE) -1 (FORMAT'YYYY-MM-DD')(CHAR(10))
  #--    ,PROS_KEY_EXPY_I = CAST(trim(:PROSKEY) as INTEGER)
  #
  #--WHERE T1.ACCT_I = T2.ACCT_I
  #--AND T1.PATY_I = T2.PATY_I
  #--AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
  #--AND (T1.ASSC_ACCT_I <> T2.ASSC_ACCT_I
  #--  OR T1.PRFR_PATY_F <> T2.PRFR_PATY_F
  #--  OR T1.SRCE_SYST_C <> T2.SRCE_SYST_C
  #--  )
  #--AND :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D
  #--AND :EXTR_D BETWEEN T2.EFFT_D AND T2.EXPY_D
   

  if snowconvert.helpers.error_code != 0:
    EXITERR()
    return
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '115' COLUMN '2' OF THE SOURCE CODE STARTING AT 'IMPORT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '115' COLUMN '2'. **
  #--IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_PROS_KEY_DATE.txt
   
  using = snowconvert.helpers.using("FILLER", "CHAR(4)", "PROSKEY", "CHAR(10)", "EXTR_D", "CHAR(10)", rows_to_read = 1)
  exec("""
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '122' COLUMN '1' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '122' COLUMN '8'. **
--INSERT INTO %%STARDATADB%%.DERV_ACCT_PATY
--SEL T1.ACCT_I

--      ,T1.PATY_I
--      ,T1.ASSC_ACCT_I
--      ,T1.PATY_ACCT_REL_C
--      ,T1.PRFR_PATY_F
--      ,T1.SRCE_SYST_C
--      ,:EXTR_D AS EFFT_D
--      ,T1.EXPY_D
--      ,CAST(trim(:PROSKEY) as INTEGER) AS PROS_KEY_EFFT_D
--      ,CASE
--          WHEN T1.EXPY_D = T1.EFFT_D THEN CAST(trim(:PROSKEY) as INTEGER)
--          ELSE 0
--       END AS PROS_KEY_EXPY_D
--      ,T1.ROW_SECU_ACCS_C
--       FROM %%DDSTG%%.DERV_ACCT_PATY_CHG T1

--       LEFT JOIN %%VTECH%%.DERV_ACCT_PATY T2
--       ON T1.ACCT_I = T2.ACCT_I
--       AND T1.PATY_I = T2.PATY_I
--       AND T1.PATY_aCCT_REL_C = T2.PATY_ACCT_REL_C
--       AND T1.ASSC_ACCT_I = T2.ASSC_ACCT_I
--       AND T1.SRCE_SYST_C = T2.SRCE_SYST_c
--       AND T1.PRFR_PATY_f = T2.PRFR_PATY_f
--       AND :EXTR_D BETWEEN t2.EFFT_D AND t2.EXPY_D

--     WHERE :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D
--       AND T2.ACCT_I IS NULL
       """, using = using)
  exec("""

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '153' COLUMN '2' OF THE SOURCE CODE STARTING AT '.'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS ';' ON LINE '151' COLUMN '8'. FAILED TOKEN WAS '.' ON LINE '153' COLUMN '2'. **
-- .
       """, using = using)
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '153' COLUMN '3' OF THE SOURCE CODE STARTING AT 'IF'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'IMPORT' ON LINE '115' COLUMN '2'. FAILED TOKEN WAS 'IF' ON LINE '153' COLUMN '3'. **
  #--IF ERRORCODE   <> 0 THEN .GOTO EXITERR
  #
  #
  #----4. Insert rows into %%STARDATADB%%.DERV_ACCT_PATY that exist now but didn't exist before 
  #
  #
  #--.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_PROS_KEY_DATE.txt
  #--USING
  #
  #--(  FILLER CHAR(4)
  #--   ,PROSKEY CHAR(10)
  #--   ,EXTR_D CHAR(10)
  #--   )
  #--INSERT INTO %%STARDATADB%%.DERV_ACCT_PATY
  #--SELECT T1.ACCT_I
  #--      ,T1.PATY_I
  #--      ,T1.ASSC_ACCT_I
  #--      ,T1.PATY_ACCT_REL_C
  #--      ,T1.PRFR_PATY_F
  #--      ,T1.SRCE_SYST_C
  #--      ,T1.EFFT_D
  #--      ,T1.EXPY_D
  #--      ,CAST(trim(:PROSKEY) as INTEGER) AS PROS_KEY_EFFT_D
  #--      ,CASE
  #--          WHEN T1.EXPY_D = T1.EFFT_D THEN CAST(trim(:PROSKEY) as INTEGER)
  #--          ELSE 0
  #--       END AS PROS_KEY_EXPY_D
  #--      ,T1.ROW_SECU_ACCS_C
  #--FROM %%DDSTG%%.DERV_ACCT_PATY_ADD T1
  #
  #--  GROUP BY 1,2,3,4,5,6,7,8,9,10,11
   

  if snowconvert.helpers.error_code != 0:
       EXITERR()
       return
  #5. Identify accounts in %%STARDATADB%%.DERV_ACCT_PATY that have ROW_SECU_ACCS_C that is different from 
  #    the current rows in ACCT_PATY and update them with the value from ACCT_PATY 
  exec("""
       !!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 190 OF FILE: /Users/psundaram/Documents/prjs/cba/agentic/cba-terradata-cdao-poc/current_state/DERV_ACCT_PATY_08_APPLY_DELTAS.bteq ***/!!!
       DELETE
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '190' COLUMN '7' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '190' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '190' COLUMN '13'. **
--              FROM  %%DDSTG%%.DERV_ACCT_PATY_ROW_SECU_FIX ALL
       """)

  if snowconvert.helpers.error_code != 0:
       EXITERR()
       return
  exec("""


-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '194' COLUMN '0' OF THE SOURCE CODE STARTING AT 'INSERT'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'INTO' ON LINE '194' COLUMN '7'. **
--INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_ROW_SECU_FIX
--SEL T1.ACCT_I
--, T1.ROW_SECU_ACCS_C AS DERV_ACCT_PATY_ROW_SECU_ACCS_C
--, T2.ROW_SECU_ACCS_C AS ACCT_PATY_ROW_SECU_ACCS_C
--FROM %%STARDATADB%%.DERV_ACCT_PATY T1
--JOIN %%DDSTG%%.DERV_ACCT_PATY_FLAG T2

--ON T1.ACCT_I = T2.ACCT_I
--AND T1.PATY_I=  T2.PATY_I
--AND T1.ROW_SECU_ACCS_C <>  T2.ROW_SECU_ACCS_C

-- GROUP BY 1,2,3
""")

  if snowconvert.helpers.error_code != 0:
EXITERR()
return
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '208' COLUMN '2' OF THE SOURCE CODE STARTING AT 'COLLECT'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'COLLECT' ON LINE '208' COLUMN '2'. **
  #
  #-- COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_ROW_SECU_FIX
   

  if snowconvert.helpers.error_code != 0:
EXITERR()
return
  exec("""

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '211' COLUMN '1' OF THE SOURCE CODE STARTING AT 'UPDATE'. EXPECTED 'Update' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '212' COLUMN '1'. **
-- UPDATE T1
--FROM %%STARDATADB%%.DERV_ACCT_PATY T1,
--%%DDSTG%%.DERV_ACCT_PATY_ROW_SECU_FIX T2

-- SET ROW_SECU_ACCS_C = T2.ACCT_PATY_ROW_SECU_ACCS_C
-- WHERE T1.ACCT_I = T2.ACCT_I
-- AND T1.ROW_SECU_ACCS_C = T2.DERV_ACCT_PATY_ROW_SECU_ACCS_C
 """)

  if snowconvert.helpers.error_code != 0:
 EXITERR()
 return
  snowconvert.helpers.quit_application(0)
  EXITERR()
  snowconvert.helpers.quit_application()
# $LastChangedBy: zakhe $
# $LastChangedDate: 2013-09-11 15:36:32 +1000 (Wed, 11 Sep 2013) $
# $LastChangedRevision: 12624 $
def EXITERR():
  snowconvert.helpers.quit_application(4)

if __name__ == "__main__":
  main()