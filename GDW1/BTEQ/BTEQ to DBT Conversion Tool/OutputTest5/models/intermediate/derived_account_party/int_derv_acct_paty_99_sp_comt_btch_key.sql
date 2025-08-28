-- =====================================================================
-- DBT Model: DERV_ACCT_PATY_99_SP_COMT_BTCH_KEY
-- Converted from BTEQ: DERV_ACCT_PATY_99_SP_COMT_BTCH_KEY.sql
-- Category: derived_account_party
-- Original Size: 1.2KB, 47 lines
-- Complexity Score: 28
-- Generated: 2025-08-21 13:42:27
-- =====================================================================

{{ intermediate_model_config() }}

-- This model performs data modification operations
-- Execute via post-hook or separate run

{{ config(
    materialized='table',
    post_hook=[
        "

------------------------------------------------------------------------------
--  Update the Batch record to show successful completion
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

-- {{ copy_from_stage(\"/cba_app/CBMGDW/{{ bteq_var(\"ENV_C\") }}/schedule/{{ bteq_var(\"STRM_C\") }}_BTCH_KEY.txt\", \"target_table\") }}
USING 
( 
  FILLER CHAR(24),
  BTCH_KEY CHAR(10) )
UPDATE {{ bteq_var(\"STARDATADB\") }}.UTIL_BTCH_ISAC           
SET
   BTCH_STUS_C = 'COMT'
  ,STUS_CHNG_S = CAST(CAST(current_timestamp as CHAR(20)) as TIMESTAMP(0))
WHERE
  BTCH_KEY_I = CAST(trim(:BTCH_KEY) as DECIMAL(10,0)) 
;  

.EXPORT RESET                  
		


 
"
    ]
) }}

-- Placeholder query for DBT execution
SELECT 1 as operation_complete
