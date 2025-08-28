.RUN FILE=${ETL_APP_BTEQ}/bteq_login.sql
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

.SET QUIET OFF
.SET ECHOREQ ON
.SET FORMAT OFF
.SET WIDTH 120

------------------------------------------------------------------------------
-- Object Name             :  prtf_tech_paty_own_rel_psst.sql
-- Object Type             :  BTEQ
--                           
-- Description             :  Persist rows for DERV_PRTF_PATY_OWN view

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
  
Delete from STAR_CAD_PROD_DATA.DERV_PRTF_PATY_OWN_REL All
;
.IF ERRORCODE <> 0    THEN .GOTO EXITERR

-- Collect stats after deletion so that the optimiser "knows" that the table is empty

 COLLECT STATS  STAR_CAD_PROD_DATA.DERV_PRTF_PATY_OWN_REL;

.IF ERRORCODE <> 0    THEN .GOTO EXITERR

Insert into STAR_CAD_PROD_DATA.DERV_PRTF_PATY_OWN_REL
( PATY_I 
, INT_GRUP_I 
, DERV_PRTF_CATG_C
 , DERV_PRTF_CLAS_C 
 , DERV_PRTF_TYPE_C 
 , PRTF_PATY_VALD_FROM_D 
 , PRTF_PATY_VALD_TO_D 
 , PRTF_PATY_EFFT_D 
 , PRTF_PATY_EXPY_D
  , PRTF_OWN_VALD_FROM_D 
  , PRTF_OWN_VALD_TO_D 
  , PRTF_OWN_EFFT_D
   , PRTF_OWN_EXPY_D
    , PTCL_N 
    , REL_MNGE_I 
    , PRTF_CODE_X 
    , DERV_PRTF_ROLE_C
     , ROLE_PLAY_TYPE_X 
     , ROLE_PLAY_I 
     , SRCE_SYST_C
      , ROW_SECU_ACCS_C
       )
       SELECT 
         PP4.PATY_I AS PATY_I 
       , PP4.INT_GRUP_I AS INT_GRUP_I  
       , PP4.DERV_PRTF_CATG_C AS DERV_PRTF_CATG_C 
       , PP4.DERV_PRTF_CLAS_C AS DERV_PRTF_CLAS_C 
       , PP4.DERV_PRTF_TYPE_C AS DERV_PRTF_TYPE_C 
       , PP4.VALD_FROM_D AS PRTF_PATY_VALD_FROM_D 
       , PP4.VALD_TO_D AS PRTF_PATY_VALD_TO_D 
       , PP4.EFFT_D AS PRTF_PATY_EFFT_D 
       , PP4.EXPY_D AS PRTF_PATY_EXPY_D 
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

FROM PVTECH.DERV_PRTF_PATY_REL PP4 
INNER JOIN PVTECH.DERV_PRTF_OWN_REL PO4 
ON PO4.INT_GRUP_I = PP4.INT_GRUP_I 
AND PO4.DERV_PRTF_TYPE_C = PP4.DERV_PRTF_TYPE_C ;

  
  .IF ERRORCODE <> 0    THEN .GOTO EXITERR
  
  COLLECT STATS  STAR_CAD_PROD_DATA.DERV_PRTF_PATY_OWN_REL;

.IF ERRORCODE <> 0    THEN .GOTO EXITERR



.QUIT 0
.LOGOFF

.LABEL EXITERR
.QUIT 1
.LOGOFF
.EXIT
