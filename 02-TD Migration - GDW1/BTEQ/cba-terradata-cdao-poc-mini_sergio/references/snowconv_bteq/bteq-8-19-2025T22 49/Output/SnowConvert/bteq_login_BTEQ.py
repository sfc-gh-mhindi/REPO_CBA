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
  #** SSC-FDM-0027 - REMOVED NEXT STATEMENT, NOT APPLICABLE IN SNOWFLAKE. LOGON **
  #.logon
   
  #** SSC-EWI-0001 - UNRECOGNIZED TOKEN ON LINE '1' COLUMN '8' OF THE SOURCE CODE STARTING AT '%'. EXPECTED 'STATEMENT' GRAMMAR. LAST MATCHING TOKEN WAS 'logon' ON LINE '1' COLUMN '2'. FAILED TOKEN WAS '%' ON LINE '1' COLUMN '8'. **
  #--%%GDW_HOST%%/%%GDW_USER%%,%%GDW_PASS%%
   
  snowconvert.helpers.quit_application()

if __name__ == "__main__":
  main()