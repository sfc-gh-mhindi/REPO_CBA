-- =====================================================================
-- DBT Model: DERV_ACCT_PATY_99_SP_COMT_BTCH_KEY
-- Converted from BTEQ: DERV_ACCT_PATY_99_SP_COMT_BTCH_KEY.sql
-- Category: derived_account_party
-- Original Size: 1.2KB, 47 lines
-- Complexity Score: 28
-- Generated: 2025-08-21 15:55:19
-- =====================================================================

{{ intermediate_model_config() }}



------------------------------------------------------------------------------
--  -- Original UPDATE removed: Update the Batch record to show successful completion
--
--  Ver  Date       Modified By            Description
--  ---- ---------- ---------------------- -----------------------------------
--  1.0  25/07/2013 Helen Zak            Initial Version
------------------------------------------------------------------------------
--
-- This info is for CBM use only
-- $LastChangedBy: zakhe $
-- $LastChangedDate: 2013-07-25 13:39:43 +1000 (Thu, 25 Jul 2013) $
-- $LastChangedRevision: 12366 $
--

USING 
( 
  FILLER CHAR(24),
  BTCH_KEY CHAR(10) )
UPDATE {{ bteq_var("STARDATADB") }}.UTIL_BTCH_ISAC           
SET
   BTCH_STUS_C = 'COMT'
  ,STUS_CHNG_S = CAST(CAST(current_timestamp as CHAR(20)) as TIMESTAMP(0))
WHERE
  BTCH_KEY_I = CAST(trim(:BTCH_KEY) as DECIMAL(10,0)) 
;
-- DBT models should be SELECT-only; UPDATE logic should be handled in post-hooks or separate processes
  

		


 
