-- =====================================================================
-- DBT Model: sp_get_btch_key
-- Converted from BTEQ: sp_get_btch_key.sql
-- Category: process_control
-- Original Size: 1.5KB, 62 lines
-- Complexity Score: 48
-- Generated: 2025-08-21 15:46:51
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


-- Remove the existing Batch Key file



CALL {{ bteq_var("STARMACRDB") }}.SP_GET_BTCH_KEY(     
  '{{ bteq_var("SRCE_SYST_M") }}'
  ,CAST({{ bteq_var("INDATE") }} as date format'yyyymmdd')              
  ,IBATCHKEY (CHAR(80))
); 

/* NOTE: BTCH_KEY is returned as a string in the format where     */
/* First 24 characters ignored then next 10 are the actual number */


-- Remove the existing DATE file

Select
  (CAST({{ bteq_var("INDATE") }} as date format'yyyymmdd'))(CHAR(8))
;

		


 
