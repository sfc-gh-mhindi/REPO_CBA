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
  #
  #  Ver  Date       Modified By            Description
  #  ---- ---------- ---------------------- -----------------------------------
  #  1.0  14/06/2013 T Jelliffe             Initial Version
  #----------------------------------------------------------------------------
  # Check pre-requisite table loads have completed (ODS and Analytics)
  # To add a new dependency, insert a PRTF_TECH_CHNG_SCHE_DEPN_TABL row and a 
  # PRTF_TECH_CHNG_SCHE_DEPN_SRCE row in UTIL_PARM and increment parm
  # CMPF_MCIB_SCHE_DEPN_DNCT_TABL by number of load processes required (eg. at
  # time of build, EVNT had 2 loads, one of which had more than one source)
  #----------------------------------------------------------------------------
  exec("""
    SELECT
      SRCE_FND
    FROM
      (
        --Check Analytics at table level
        SELECT
          1 AS SRCE_FND
        FROM
          (
            SELECT
              --AND UPI.SRCE_SYST_M = 'SAP'
              UPI.TRGT_M AS TRGT_M AS TRGT_M,
              UPI.SRCE_M as SRCE_M AS SRCE_M
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '34' COLUMN '8' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '34' COLUMN '8'. FAILED TOKEN WAS '%' ON LINE '34' COLUMN '13'. **
--       FROM %%VTECH%%.UTIL_PROS_ISAC AS UPI
            -- Want the data loaded for yesterday
            WHERE
              UPI.BTCH_RUN_D = CURRENT_DATE() - 1
              AND UPI.TRGT_M IN
                                --** SSC-FDM-0002 - CORRELATED SUBQUERIES MAY HAVE SOME FUNCTIONAL DIFFERENCES. **
                                (
                                  --Get the list of pre-requisite tables 
                                  SELECT
                                    UP1.PARM_LTRL_STRG_X
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '42' COLUMN '29' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '42' COLUMN '29'. FAILED TOKEN WAS '%' ON LINE '42' COLUMN '34'. **
--                            FROM %%VTECH%%.UTIL_PARM UP1
                                  WHERE
                                    UPPER(RTRIM(UP1.PARM_M)) = UPPER(RTRIM('PRTF_TECH_CHNG_SCHE_DEPN_TABL'))
                                )
              --Get the list of pre-requisite sources
              AND UPI.SRCE_M LIKE ANY (
                                  SELECT
                                    '%' || TRIM(UP1.PARM_LTRL_STRG_X) || '%' AS SRCE_M
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '49' COLUMN '12' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '49' COLUMN '12'. FAILED TOKEN WAS '%' ON LINE '49' COLUMN '17'. **
--           FROM %%VTECH%%.UTIL_PARM UP1
                                  WHERE
                                    UPPER(RTRIM(UP1.PARM_M)) = UPPER(RTRIM('PRTF_TECH_CHNG_SCHE_DEPN_SRCE'))
              )
              --and check they have been loaded
              AND UPPER(RTRIM(UPI.COMT_F)) = UPPER(RTRIM('Y'))
            QUALIFY
              RANK() OVER (
              PARTITION BY
                                  UPI.TRGT_M
              ORDER BY UPI.BTCH_RUN_D DESC NULLS LAST) = 1
          ) DT
        --and check all relevant sources have loaded
        HAVING
          COUNT(TRGT_M) =
          --** SSC-FDM-0002 - CORRELATED SUBQUERIES MAY HAVE SOME FUNCTIONAL DIFFERENCES. **
          (
            SELECT
              ANY_VALUE(UP2.PARM_LTRL_N)
-- ** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '64' COLUMN '10' OF THE SOURCE CODE STARTING AT 'FROM'. EXPECTED 'FROM' GRAMMAR. LAST MATCHING TOKEN WAS 'FROM' ON LINE '64' COLUMN '10'. FAILED TOKEN WAS '%' ON LINE '64' COLUMN '15'. **
--         FROM %%VTECH%%.UTIL_PARM UP2
            WHERE
              UPPER(RTRIM(UP2.PARM_M)) = UPPER(RTRIM('PRTF_TECH_CHNG_SCHE_DEPN_SRCE_LOAD'))
          )
      ) SRCES
    GROUP BY
      1
    HAVING
      COUNT(SRCE_FND) = 1
    """)

  # This info is for CBM use only
  # $LastChangedBy: jelifft $
  # $LastChangedDate: 2013-07-04 09:44:07 +1000 (Thu, 04 Jul 2013) $
  # $LastChangedRevision: 12230 $
  if snowconvert.helpers.error_code != 0:
    EXITERR()
    return

  if snowconvert.helpers.activity_count == 0:
    REPOLL()
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
   
  REPOLL()
def REPOLL():
  snowconvert.helpers.quit_application(2)
  #** SSC-FDM-0027 - REMOVED NEXT STATEMENT, NOT APPLICABLE IN SNOWFLAKE. LOGOFF **
  #.LOGOFF
   
  snowconvert.helpers.quit_application()

if __name__ == "__main__":
  main()