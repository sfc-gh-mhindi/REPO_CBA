-- =====================================================
-- CBA NPW-DBT Control Data Setup Script
-- =====================================================
-- This script creates and populates the control tables
-- required by the dbt models
-- =====================================================

USE DATABASE NPD_D12_DMN_GDWMIG_IBRG_V;
USE SCHEMA P_V_OUT_001_STD_0;

-- =====================================================
-- 2. INSERT REQUIRED CONTROL DATA
-- =====================================================

-- Insert stream template entries for CSE_ICE_BUS_DEAL
INSERT INTO NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.RUN_STRM_TMPL (RUN_STRM_C, SYST_C, RUN_STRM_ABRT_F, RUN_STRM_ACTV_F)
VALUES 
    ('CSE_ICE_BUS_DEAL', 'CSEL4', 'N', 'I'),
    ('CSE_CPL_BUS_APP', 'CSEL4', 'N', 'I'),
    ('CSE_CCC_BUS_APP_PROD', 'CSEL4', 'N', 'I'),
    ('CSE_COM_BUS_CCL_CHL_COM_APP', 'CSEL4', 'N', 'I'),
    ('CSE_COM_BUS_APP_PROD_CCL_PL_APP_PROD', 'CSEL4', 'N', 'I'),
    ('CSE_COM_CPO_BUS_NCPR_CLNT', 'CSEL4', 'N', 'I'),
    --added by MH
    ('CSE_L4_PRE_PROC', 'CSEL4', 'N', 'I');
-- ON DUPLICATE KEY UPDATE 
--     RUN_STRM_DESC = VALUES(RUN_STRM_DESC),
--     RECD_UPDT_S = CURRENT_TIMESTAMP;

-- Insert ETL processing dates for streams
INSERT INTO NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.RUN_STRM_ETL_D (RS_M, ETL_D)
VALUES 
    ('CSE_ICE_BUS_DEAL', '2006-01-01'),
    ('CSE_ICE_BUS_DEAL', CURRENT_DATE),
    ('CSE_CPL_BUS_APP', '2006-06-16'),
    ('CSE_CPL_BUS_APP', CURRENT_DATE),
    ('CSE_CCC_BUS_APP_PROD', '2006-01-01'),
    ('CSE_CCC_BUS_APP_PROD', CURRENT_DATE),
    ('CSE_COM_BUS_CCL_CHL_COM_APP', '2006-01-01'),
    ('CSE_COM_BUS_CCL_CHL_COM_APP', CURRENT_DATE),
    ('CSE_COM_BUS_APP_PROD_CCL_PL_APP_PROD', '2017-03-06'),
    ('CSE_COM_BUS_APP_PROD_CCL_PL_APP_PROD', CURRENT_DATE),
    ('CSE_COM_CPO_BUS_NCPR_CLNT', '2010-10-30'),
    ('CSE_COM_CPO_BUS_NCPR_CLNT', CURRENT_DATE);

-- =====================================================
-- 3. SAMPLE STEP OCCURRENCE DATA
-- =====================================================

-- Insert sample step occurrence data for testing
INSERT INTO NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.STEP_OCCR (
    STEP_OCCR_ID, RUN_STRM_OCCR_ID, RUN_STRM_C, STEP_C, STEP_STUS_C,
    STEP_OCCR_STRT_S, RECD_CRAT_S, STEP_SQNO
)
VALUES 
    ('S10_VAL_DEAL20060101', 'CSE_ICE_BUS_DEAL20060101', 'CSE_ICE_BUS_DEAL', 'V', 'C', 
     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1),
    ('S10_VAL_DEAL' || TO_CHAR(CURRENT_DATE, 'YYYYMMDD'), 
     'CSE_ICE_BUS_DEAL' || TO_CHAR(CURRENT_DATE, 'YYYYMMDD'), 
     'CSE_ICE_BUS_DEAL', 'V', 'R', 
     CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1);

     