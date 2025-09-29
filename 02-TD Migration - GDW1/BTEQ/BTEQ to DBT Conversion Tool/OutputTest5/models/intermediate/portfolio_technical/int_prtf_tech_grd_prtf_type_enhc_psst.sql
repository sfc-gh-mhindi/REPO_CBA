-- =====================================================================
-- DBT Model: prtf_tech_grd_prtf_type_enhc_psst
-- Converted from BTEQ: prtf_tech_grd_prtf_type_enhc_psst.sql
-- Category: portfolio_technical
-- Original Size: 1.9KB, 80 lines
-- Complexity Score: 58
-- Generated: 2025-08-21 13:42:27
-- =====================================================================

{{ intermediate_model_config() }}


                                             
------------------------------------------------------------------------------
--
--
--  Ver  Date       Modified By            Description
--  ---- ---------- ---------------------- -----------------------------------
--  1.0  15/07/2013 T Jelliffe             Initial Version
--  1.5  01/11/2013 T Jelliffe             Add the HIST persisted table
------------------------------------------------------------------------------

-- PDGRD DATA

Delete from {{ bteq_var("DGRDDB") }}.GRD_PRTF_TYPE_ENHC_PSST All
;

Collect Statistics on {{ bteq_var("DGRDDB") }}.GRD_PRTF_TYPE_ENHC_PSST;

-- Original INSERT converted to SELECT for DBT intermediate model
Select
   GP.PERD_D
  ,GP.PRTF_TYPE_C
  ,GP.PRTF_TYPE_M
  ,GP.PRTF_CLAS_C
  ,GP.PRTF_CLAS_M
  ,GP.PRTF_CATG_C
  ,GP.PRTF_CATG_M
From                
  {{ bteq_var("VTECH") }}.GRD_PRTF_TYPE_ENHC GP;

Collect Statistics on {{ bteq_var("DGRDDB") }}.GRD_PRTF_TYPE_ENHC_PSST;



--< Populate the HISTORY version of the table >--
Delete from {{ bteq_var("DGRDDB") }}.GRD_PRTF_TYPE_ENHC_HIST_PSST
;

-- Original INSERT converted to SELECT for DBT intermediate model
Select
   G.PRTF_TYPE_C                   
  ,G.PRTF_TYPE_M                   
  ,G.PRTF_CLAS_C                   
  ,G.PRTF_CLAS_M                   
  ,G.PRTF_CATG_C                   
  ,G.PRTF_CATG_M    
	,MIN(PERD_D) as VALD_FROM_D
  ,MAX(PERD_D) as VALD_TO_D
From
  {{ bteq_var("VTECH") }}.GRD_PRTF_TYPE_ENHC_PSST G
Group By 1,2,3,4,5,6;

Collect Statistics on {{ bteq_var("DGRDDB") }}.GRD_PRTF_TYPE_ENHC_HIST_PSST;





 
