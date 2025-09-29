-- =====================================================================
-- DBT Model: sp_get_pros_key
-- Converted from BTEQ: sp_get_pros_key.sql
-- Category: process_control
-- Original Size: 1.5KB, 59 lines
-- Complexity Score: 44
-- Generated: 2025-08-21 13:45:01
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
-- $LastChangedDate: 2013-07-03 15:57:40 +1000 (Wed, 03 Jul 2013) $
-- $LastChangedRevision: 12229 $
--

.EXPORT RESET     

-- Remove the existing DATE file

-- {{ copy_from_stage(\"/cba_app/CBMGDW/{{ bteq_var(\"ENV_C\") }}/schedule/{{ bteq_var(\"STRM_C\") }}_BTCH_KEY.txt\", \"target_table\") }}

-- {{ copy_to_stage(\"source_table\", \"/cba_app/CBMGDW/{{ bteq_var(\"ENV_C\") }}/schedule/{{ bteq_var(\"STRM_C\") }}_{{ bteq_var(\"TBSHORT\") }}PROS_KEY.txt\") }}

USING 
( 
  FILLER CHAR(24),
  BTCHKEY CHAR(10) )
CALL {{ bteq_var(\"STARMACRDB\") }}.SP_GET_PROS_KEY(        
  '{{ bteq_var(\"GDW_USER\") }}',           -- From the config.pass parameters
  '{{ bteq_var(\"SRCE_SYST_M\") }}',          
  '{{ bteq_var(\"SRCE_M\") }}',           
  '{{ bteq_var(\"PSST_TABLE_M\") }}',           
  CAST(trim(:BTCHKEY) as DECIMAL(10,0)),
  CAST({{ bteq_var(\"INDATE\") }} as DATE FORMAT'YYYYMMDD'),            
  '{{ bteq_var(\"RSTR_F\") }}',                -- Restart Flag
  'MVS',                     
  IPROCESSKEY (CHAR(100))
);  

.EXPORT RESET                  
                		


 
"
    ]
) }}

-- Placeholder query for DBT execution
SELECT 1 as operation_complete
