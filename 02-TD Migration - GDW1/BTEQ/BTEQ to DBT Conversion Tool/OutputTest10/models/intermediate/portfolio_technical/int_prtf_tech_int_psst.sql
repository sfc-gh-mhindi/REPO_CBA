-- =====================================================================
-- DBT Model: prtf_tech_int_psst
-- Converted from BTEQ: prtf_tech_int_psst.sql
-- Category: portfolio_technical
-- Original Size: 2.2KB, 78 lines
-- Complexity Score: 39
-- Generated: 2025-08-21 15:46:51
-- =====================================================================

{{ intermediate_model_config() }}



------------------------------------------------------------------------------
--
--  Ver  Date       Modified By        Description
--  ---- ---------- ------------------ ---------------------------------------
--  1.0  11/06/2013 T Jelliffe         Initial Version
--  1.1  11/07/2013 T Jelliffe         Use PROS_KEY_EFFT_I prevent self join
--  1.2  12/07/2013 T Jelliffe         Time period reduced 15 to 3 years
--  1.3  27/08/2013 T Jelliffe         Use only records with same EFFT_D
--  1.4  21/10/2013 T Jelliffe         Insert/-- Original DELETE removed: Delete changed records
--  1.5  01/11/2013 T Jelliffe         Bug fixes - Row_Number not Rank
------------------------------------------------------------------------------
-- This info is for CBM use only
-- $LastChangedBy: jelifft $
-- $LastChangedDate: 2013-11-01 14:10:34 +1100 (Fri, 01 Nov 2013) $
-- $LastChangedRevision: 12954 $
--

--<================================================>--
--< STEP 4 Delete all the original overlap records >--
--<================================================>--

Delete
	A
From 
	 {{ bteq_var("STARDATADB") }}.DERV_PRTF_INT_PSST A
	,{{ bteq_var("VTECH") }}.DERV_PRTF_INT_HIST_PSST B
Where  
  A.INT_GRUP_I = B.INT_GRUP_I
  And (A.JOIN_FROM_D,A.JOIN_TO_D) overlaps (B.JOIN_FROM_D,B.JOIN_TO_D)   
;
-- DBT handles table replacement via materialization strategy

--<===============================================>--
--< STEP 5 - Insert all deduped records into base >--
--<===============================================>--

-- Original INSERT converted to SELECT for DBT intermediate model
Select
   A.INT_GRUP_I                    
  ,A.INT_GRUP_TYPE_C               
  ,A.JOIN_FROM_D                   
  ,A.JOIN_TO_D    
  ,A.EFFT_D
  ,A.EXPY_D                 
  ,A.PTCL_N                        
  ,A.REL_MNGE_I                    
  ,A.VALD_FROM_D                   
  ,A.VALD_TO_D          
  ,0 as PROS_KEY_I         
From 
  {{ bteq_var("VTECH") }}.DERV_PRTF_INT_HIST_PSST A;

Collect Statistics on {{ bteq_var("STARDATADB") }}.DERV_PRTF_INT_PSST;








