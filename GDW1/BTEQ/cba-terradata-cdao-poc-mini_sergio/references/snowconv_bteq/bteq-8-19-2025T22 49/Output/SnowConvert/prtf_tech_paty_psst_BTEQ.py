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
  #  1.0  18/07/2013 T Jelliffe         Initial Version
  #  1.1  27/08/2013 T Jelliffe         Use only records with same EFFT_D
  #  1.2  01/11/2013 T Jelliffe         Bug fixes - Row_Number not Rank
  #  1.3  27/11/2013 T Jelliffe         Fix overlap calc, JOIN_DATE to calendar
  #----------------------------------------------------------------------------
  # This info is for CBM use only
  # $LastChangedBy: jelifft $
  # $LastChangedDate: 2013-11-27 14:06:55 +1100 (Wed, 27 Nov 2013) $
  # $LastChangedRevision: 13104 $
  #
  #<============================================>--
  #< STEP 6 - Rule 1 Multiple party to IG       >--
  #<============================================>--
  exec("""
    !!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 27 OF FILE: /Users/psundaram/Documents/prjs/cba/agentic/cba-terradata-cdao-poc/current_state/prtf_tech_paty_psst.bteq ***/!!!
    Delete
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '27' COLUMN '7' OF THE SOURCE CODE STARTING AT 'From'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'From' ON LINE '27' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '27' COLUMN '12'. **
--           From %%STARDATADB%%.DERV_PRTF_PATY_INT_GRUP_PSST
    """)

  if snowconvert.helpers.error_code != 0:
    EXITERR()
    return
  exec("""

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '31' COLUMN '0' OF THE SOURCE CODE STARTING AT 'Insert'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'into' ON LINE '31' COLUMN '7'. **
--Insert into %%STARDATADB%%.DERV_PRTF_PATY_INT_GRUP_PSST
--Select
--	 DT2.INT_GRUP_I
--	,DT2.PATY_I
--	,DT2.EFFT_D
--	,DT2.EXPY_D
--	,DT2.VALD_FROM_D
--	,DT2.VALD_TO_D
--	,DT2.PERD_D
--	,DT2.REL_C
--	,DT2.SRCE_SYST_C
--	,ROW_NUMBER() OVER (Partition By DT2.PATY_I, DT2.REL_C  Order by DT2.PERD_D ) as GRUP_N
--	,DT2.ROW_SECU_ACCS_C
--	,DT2.PROS_KEY_I
--From
--	(
--		Select
--				 DT1.INT_GRUP_I
--				,DT1.PATY_I
--				,C.CALENDAR_DATE as PERD_D
--				,DT1.EFFT_D
--				,DT1.EXPY_D
--				,DT1.VALD_FROM_D
--				,DT1.VALD_TO_D
--				,DT1.REL_C
--				,DT1.SRCE_SYST_C
--				,DT1.ROW_SECU_ACCS_C
--				,DT1.PROS_KEY_I
--		From
--			(
--				Select
--           A.INT_GRUP_I
--          ,A.PATY_I
--          ,A.JOIN_FROM_D
--          ,A.JOIN_TO_D
--          ,A.VALD_FROM_D
--          ,A.VALD_TO_D
--          ,A.EFFT_D
--          ,A.EXPY_D
--          ,A.REL_C
--          ,A.SRCE_SYST_C
--          ,A.ROW_SECU_ACCS_C
--          ,A.PROS_KEY_I
--				From
--					%%VTECH%%.DERV_PRTF_PATY_PSST A
--					Inner Join %%VTECH%%.DERV_PRTF_PATY_PSST B
--					On A.PATY_I = B.PATY_I
--					and A.REL_C = B.REL_C
----					And A.INT_GRUP_I <> B.INT_GRUP_I
--					And (A.JOIN_FROM_D,A.JOIN_TO_D) Overlaps (B.JOIN_FROM_D, B.JOIN_TO_D)
--          And (
--            A.JOIN_FROM_D <> B.JOIN_FROM_D
--            Or A.JOIN_TO_D <> B.JOIN_TO_D

--            -- New record
--            Or A.INT_GRUP_I <> B.INT_GRUP_I
--          )

--			) DT1

--			Inner Join %%VTECH%%.CALENDAR C
--			--On C.CALENDAR_DATE between DT1.VALD_FROM_D and DT1.VALD_TO_D
--			On C.CALENDAR_DATE between DT1.JOIN_FROM_D and DT1.JOIN_TO_D
--			And C.CALENDAR_DATE between ADD_MONTHS((Current_Date - EXTRACT(DAY from CURRENT_DATE) +1 ),-39) and ADD_MONTHS(Current_Date, 1)

--		Qualify Row_Number() Over( Partition By DT1.PATY_I, DT1.REL_C, C.CALENDAR_DATE Order BY DT1.EFFT_D Desc, DT1.INT_GRUP_I Desc) = 1

--	) DT2
	""")

  if snowconvert.helpers.error_code != 0:
	EXITERR()
	return
  #<==========================================>--
  #< STEP 7 - Do the history version of above >--
  #<==========================================>--
  exec("""
	!!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 108 OF FILE: /Users/psundaram/Documents/prjs/cba/agentic/cba-terradata-cdao-poc/current_state/prtf_tech_paty_psst.bteq ***/!!!
	Delete
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '108' COLUMN '7' OF THE SOURCE CODE STARTING AT 'from'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'from' ON LINE '108' COLUMN '7'. FAILED TOKEN WAS '%' ON LINE '108' COLUMN '12'. **
--	       from %%STARDATADB%%.DERV_PRTF_PATY_HIST_PSST All
	""")

  if snowconvert.helpers.error_code != 0:
	EXITERR()
	return
  exec("""


-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '113' COLUMN '0' OF THE SOURCE CODE STARTING AT 'Insert'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'into' ON LINE '113' COLUMN '7'. **
--Insert into %%STARDATADB%%.DERV_PRTF_PATY_HIST_PSST
--Select Distinct
--   DT2.INT_GRUP_I
--  ,DT2.PATY_I
--  ,DT2.REL_C
--  ,MIN(DT2.PERD_D) Over ( Partition By DT2.PATY_I, DT2.INT_GRUP_I, DT2.GRUP_N ) as JOIN_FROM_D
--  ,MAX(DT2.PERD_D) Over ( Partition By DT2.PATY_I, DT2.INT_GRUP_I, DT2.GRUP_N ) as JOIN_TO_D
--  ,DT2.VALD_FROM_D
--  ,DT2.VALD_TO_D
--  ,DT2.EFFT_D
--  ,DT2.EXPY_D
--  ,DT2.SRCE_SYST_C
--  ,DT2.ROW_SECU_ACCS_C
--  ,DT2.PROS_KEY_I
--From
--  (
--  Select
--     C.INT_GRUP_I
--    ,C.PATY_I
--    ,C.REL_C
--    ,C.VALD_FROM_D
--    ,C.VALD_TO_D
--    ,C.EFFT_D
--    ,C.EXPY_D
--    ,C.SRCE_SYST_C
--    ,C.ROW_SECU_ACCS_C
--    ,C.PERD_D
--    ,MAX(Coalesce( DT1.ROW_N, 0 )) Over (Partition By C.INT_GRUP_I, C.PATY_I, C.REL_C, C.PERD_D) as GRUP_N
--    ,C.PROS_KEY_I
--  From
--    %%VTECH%%.DERV_PRTF_PATY_INT_GRUP_PSST C
--    Left Join (
--      -- Detect the change in non-key values between rows
--      Select
--         A.INT_GRUP_I
--        ,A.PATY_I
--        ,A.REL_C
--        ,A.ROW_N
--        ,A.PERD_D
--        ,A.EFFT_D
--        ,A.EXPY_D
--      From
--        %%VTECH%%.DERV_PRTF_PATY_INT_GRUP_PSST A
--        Inner Join %%VTECH%%.DERV_PRTF_PATY_INT_GRUP_PSST B
--        On A.PATY_I = B.PATY_I
--        And A.REL_C = B.REL_C
--        And A.ROW_N = B.ROW_N + 1
--        And A.EFFT_D <> B.EFFT_D

--    ) DT1
--    On C.INT_GRUP_I = DT1.INT_GRUP_I
--    And C.PATY_I = DT1.PATY_I
--    And C.REL_C = DT1.REL_C
--    And C.ROW_N >= DT1.ROW_N
--    And DT1.PERD_D <= C.PERD_D
--  ) DT2
    """)

  if snowconvert.helpers.error_code != 0:
    EXITERR()
    return
  #<================================================>--
  #< STEP 8 Delete all the original overlap records >--
  #<================================================>--
  exec("""
    !!!RESOLVE EWI!!! /*** SSC-EWI-0013 - EXCEPTION THROWN WHILE CONVERTING ITEM: Mobilize.T12Data.Sql.Ast.TdDelete. LINE: 177 OF FILE: /Users/psundaram/Documents/prjs/cba/agentic/cba-terradata-cdao-poc/current_state/prtf_tech_paty_psst.bteq ***/!!!
    Delete A
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '179' COLUMN '1' OF THE SOURCE CODE STARTING AT 'From'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'From' ON LINE '179' COLUMN '1'. FAILED TOKEN WAS '%' ON LINE '180' COLUMN '3'. **
--From
--	 %%STARDATADB%%.DERV_PRTF_PATY_PSST A
--	,%%VTECH%%.DERV_PRTF_PATY_HIST_PSST B
	 Where
      A.PATY_I = B.PATY_I
      And A.REL_C = B.REL_C
      And PUBLIC.PERIOD_OVERLAPS_UDF(ARRAY_CONSTRUCT((A.JOIN_FROM_D, A.JOIN_TO_D), (B.JOIN_FROM_D, B.JOIN_TO_D))) !!!RESOLVE EWI!!! /*** SSC-EWI-TD0053 - SNOWFLAKE DOES NOT SUPPORT THE PERIOD DATATYPE, ALL PERIODS ARE HANDLED AS VARCHAR INSTEAD ***/!!!
	 """)

  if snowconvert.helpers.error_code != 0:
	 EXITERR()
	 return
  #<========>--
  #< STEP 9 >--
  #< Insert the updated records with corrected VALD dates
  #<========>--
  exec("""

-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '194' COLUMN '0' OF THE SOURCE CODE STARTING AT 'Insert'. EXPECTED 'Insert' GRAMMAR. LAST MATCHING TOKEN WAS 'into' ON LINE '194' COLUMN '7'. **
--	 --<========>--
----< STEP 9 >--
----< Insert the updated records with corrected VALD dates
----<========>--
--Insert into %%STARDATADB%%.DERV_PRTF_PATY_PSST
--Select
--   INT_GRUP_I
--  ,PATY_I
--  ,JOIN_FROM_D
--  ,(Case
--      WHEN JOIN_TO_D = ADD_MONTHS(CURRENT_DATE, 1) Then ('9999-12-31'(Date,format'yyyy-mm-dd'))
--      ELSE JOIN_TO_D
--    End
--    ) as JOIN_TO_D
--  ,EFFT_D
--  ,EXPY_D
--  ,VALD_FROM_D
--  ,VALD_TO_D
--  ,REL_C
--  ,SRCE_SYST_C
--  ,ROW_SECU_ACCS_C
--  ,PROS_KEY_I
--From
--  %%VTECH%%.DERV_PRTF_PATY_HIST_PSST
  """)

  if snowconvert.helpers.error_code != 0:
  EXITERR()
  return
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '218' COLUMN '1' OF THE SOURCE CODE STARTING AT 'Collect'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'Collect' ON LINE '218' COLUMN '1'. **
  #
  #
  #--Collect Statistics on %%STARDATADB%%.DERV_PRTF_PATY_PSST
   

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