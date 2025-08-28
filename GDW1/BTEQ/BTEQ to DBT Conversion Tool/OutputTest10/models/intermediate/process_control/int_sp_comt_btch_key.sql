-- =====================================================================
-- DBT Model: sp_comt_btch_key
-- Converted from BTEQ: sp_comt_btch_key.sql
-- Category: process_control
-- Original Size: 1.2KB, 47 lines
-- Complexity Score: 28
-- Generated: 2025-08-21 15:46:51
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
--  1.0  11/06/2013 T Jelliffe             Initial Version
------------------------------------------------------------------------------
--
-- This info is for CBM use only
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-07-04 12:06:00 +1000 (Thu, 04 Jul 2013) $
-- $LastChangedRevision: 12239 $
--

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

		


 
"
    ]
) }}

-- Placeholder query for DBT execution
SELECT 1 as operation_complete
