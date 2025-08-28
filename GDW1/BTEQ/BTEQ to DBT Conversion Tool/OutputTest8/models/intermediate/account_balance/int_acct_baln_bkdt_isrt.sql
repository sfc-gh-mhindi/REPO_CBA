-- =====================================================================
-- DBT Model: ACCT_BALN_BKDT_ISRT
-- Converted from BTEQ: ACCT_BALN_BKDT_ISRT.sql
-- Category: account_balance
-- Original Size: 1.9KB, 67 lines
-- Complexity Score: 23
-- Generated: 2025-08-21 13:57:48
-- =====================================================================

{{ intermediate_model_config() }}


----------------------------------------------------------------------
-- $LastChangedBy: vajapes $
-- $LastChangedDate: 2012-02-28 09:09:17 +1100 (Tue, 28 Feb 2012) $
-- $LastChangedRevision: 9222 $
----------------------------------------------------------------------
------------------------------------------------------------------------------
--
--  Description : Populate ACCT's into ACCT BALN with the modified adjustments
--
--   Ver  Date       Modified By            Description
--  ---- ---------- ---------------------- -----------------------------------
--  1.0  2011-10-05 Suresh Vajapeyajula               Initial Version
------------------------------------------------------------------------------

/*Inserting the data  into ACCT_BALN_BKDT from  ACCT_BALN_BKDT_STG2*/
-- Original INSERT converted to SELECT for DBT intermediate model
SELECT 
ACCT_I,                        
BALN_TYPE_C,                   
CALC_FUNC_C,                   
TIME_PERD_C,                   
BALN_A,                        
CALC_F,                        
SRCE_SYST_C,                   
ORIG_SRCE_SYST_C,              
LOAD_D,                        
BKDT_EFFT_D,                   
BKDT_EXPY_D,                  
PROS_KEY_EFFT_I,               
PROS_KEY_EXPY_I,               
BKDT_PROS_KEY_I
FROM
{{ bteq_var("DDSTG") }}.ACCT_BALN_BKDT_STG2;



