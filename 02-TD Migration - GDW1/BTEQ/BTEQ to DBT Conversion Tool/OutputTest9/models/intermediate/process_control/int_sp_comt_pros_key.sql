-- =====================================================================
-- DBT Model: sp_comt_pros_key
-- Converted from BTEQ: sp_comt_pros_key.sql
-- Category: process_control
-- Original Size: 1.3KB, 52 lines
-- Complexity Score: 31
-- Generated: 2025-08-21 14:21:55
-- =====================================================================

{{ intermediate_model_config() }}

-- This model performs data modification operations
-- Execute via post-hook or separate run

{{ config(
    materialized='table',
    post_hook=[
        "

------------------------------------------------------------------------------
--
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
  FILLER CHAR(13),
  PROS_KEY CHAR(11) )
UPDATE {{ bteq_var(\"STARDATADB\") }}.UTIL_PROS_ISAC           
SET
   SUCC_F  = 'Y'
  ,COMT_F = 'Y'
  ,COMT_S = CAST(CAST(current_timestamp as CHAR(20)) as TIMESTAMP(0))
WHERE
  PROS_KEY_I = CAST(trim(:PROS_KEY) as DECIMAL(10,0)) 
  AND SRCE_SYST_M = '{{ bteq_var(\"SRCE_M\") }}'
  AND TRGT_M = '{{ bteq_var(\"PSST_TABLE_M\") }}'
  AND BTCH_RUN_D = CAST({{ bteq_var(\"INDATE\") }} as DATE FORMAT'YYYYMMDD')
;  


		


 
"
    ]
) }}

-- Placeholder query for DBT execution
SELECT 1 as operation_complete
