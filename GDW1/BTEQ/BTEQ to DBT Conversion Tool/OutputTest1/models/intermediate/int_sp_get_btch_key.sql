-- =====================================================================
-- DBT Model: sp_get_btch_key
-- Converted from BTEQ: sp_get_btch_key.sql
-- Category: data_loading
-- Original Size: 1.5KB, 62 lines
-- Complexity Score: 48
-- Generated: 2025-08-20 17:43:49
-- =====================================================================

{{ intermediate_model_config() }}



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
-- $LastChangedDate: 2013-09-02 13:49:23 +1000 (Mon, 02 Sep 2013) $
-- $LastChangedRevision: 12581 $
--

.EXPORT RESET     

-- Remove the existing Batch Key file

-- {{ copy_to_stage("source_table", "/cba_app/CBMGDW/{{ bteq_var("ENV_C") }}/schedule/{{ bteq_var("STRM_C") }}_BTCH_KEY.txt") }}


CALL {{ bteq_var("STARMACRDB") }}.SP_GET_BTCH_KEY(     
  '{{ bteq_var("SRCE_SYST_M") }}'
  ,CAST({{ bteq_var("INDATE") }} as date format'yyyymmdd')              
  ,IBATCHKEY (CHAR(80))
); 

/* NOTE: BTCH_KEY is returned as a string in the format where     */
/* First 24 characters ignored then next 10 are the actual number */

.EXPORT RESET   

-- Remove the existing DATE file

-- {{ copy_to_stage("source_table", "/cba_app/CBMGDW/{{ bteq_var("ENV_C") }}/schedule/{{ bteq_var("STRM_C") }}_DATE.txt") }}
Select
  (CAST({{ bteq_var("INDATE") }} as date format'yyyymmdd'))(CHAR(8))
;

		

.QUIT 0
.LOGOFF

.LABEL EXITERR
.QUIT 1
.LOGOFF
.EXIT 
 
