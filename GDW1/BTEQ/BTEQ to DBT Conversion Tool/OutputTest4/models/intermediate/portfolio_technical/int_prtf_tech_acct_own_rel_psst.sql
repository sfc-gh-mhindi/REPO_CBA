-- =====================================================================
-- DBT Model: prtf_tech_acct_own_rel_psst
-- Converted from BTEQ: prtf_tech_acct_own_rel_psst.sql
-- Category: portfolio_technical
-- Original Size: 3.0KB, 106 lines
-- Complexity Score: 40
-- Generated: 2025-08-21 11:23:07
-- =====================================================================

{{ intermediate_model_config() }}



------------------------------------------------------------------------------
-- Object Name             :  prtf_tech_acct_own_rel_psst.sql
-- Object Type             :  BTEQ
--                           
-- Description             :  Persist rows for DERV_PRTF_ACCT_OWN view

------------------------------------------------------------------------------
--   Modification history

------------------------------------------------------------------------------                       --
--  Ver  Date                   Modified By        Description
--  ---- -----------------    ------------             ---------------------------------------
--  1.0  09/01/2014    Helen Zak            Initial Version

------------------------------------------------------------------------------
-- This info is for CBM use only
-- $LastChangedBy: zakhe $
-- $LastChangedDate: 2014-01-10 14:56:29 +1100 (Fri, 10 Jan 2014) $
-- $LastChangedRevision: 13159 $
--
  
Delete from {{ bteq_var("STARDATADB") }}.DERV_PRTF_ACCT_OWN_REL All
;

-- Collect stats after deletion so that the optimiser "knows" that the table is empty

 COLLECT STATS  {{ bteq_var("STARDATADB") }}.DERV_PRTF_ACCT_OWN_REL;


-- Original INSERT converted to SELECT for DBT intermediate model
SELECT 
      PP4.ACCT_I AS ACCT_I 
    , PP4.INT_GRUP_I AS INT_GRUP_I  
    , PP4.DERV_PRTF_CATG_C AS DERV_PRTF_CATG_C 
    , PP4.DERV_PRTF_CLAS_C AS DERV_PRTF_CLAS_C 
    , PP4.DERV_PRTF_TYPE_C AS DERV_PRTF_TYPE_C 
    , PP4.VALD_FROM_D AS PRTF_ACCT_VALD_FROM_D 
    , PP4.VALD_TO_D AS PRTF_ACCT_VALD_TO_D 
    , PP4.EFFT_D AS PRTF_ACCT_EFFT_D 
    , PP4.EXPY_D AS PRTF_ACCT_EXPY_D 
    , PO4.VALD_FROM_D AS PRTF_OWN_VALD_FROM_D 
    , PO4.VALD_TO_D AS PRTF_OWN_VALD_TO_D 
    , PO4.EFFT_D AS PRTF_OWN_EFFT_D
    , PO4.EXPY_D AS PRTF_OWN_EXPY_D 
    , PP4.PTCL_N AS PTCL_N 
    , PP4.REL_MNGE_I AS REL_MNGE_I 
    , PP4.PRTF_CODE_X AS PRTF_CODE_X 
    , PO4.DERV_PRTF_ROLE_C AS DERV_PRTF_ROLE_C
    , PO4.ROLE_PLAY_TYPE_X AS ROLE_PLAY_TYPE_X 
    , PO4.ROLE_PLAY_I AS ROLE_PLAY_I
    , PP4.SRCE_SYST_C AS SRCE_SYST_C 
    , PP4.ROW_SECU_ACCS_C AS ROW_SECU_ACCS_C 

FROM {{ bteq_var("VTECH") }}.DERV_PRTF_ACCT_REL PP4 
INNER JOIN {{ bteq_var("VTECH") }}.DERV_PRTF_OWN_REL PO4 
ON PO4.INT_GRUP_I = PP4.INT_GRUP_I 
AND PO4.DERV_PRTF_TYPE_C = PP4.DERV_PRTF_TYPE_C;

  
  
COLLECT STATS  {{ bteq_var("STARDATADB") }}.DERV_PRTF_ACCT_OWN_REL;




.QUIT 0
.LOGOFF

.LABEL EXITERR
.QUIT 1
.LOGOFF
.EXIT
