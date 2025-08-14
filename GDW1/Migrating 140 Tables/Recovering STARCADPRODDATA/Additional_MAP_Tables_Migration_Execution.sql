-- =====================================================================
-- ADDITIONAL MAP TABLES MIGRATION EXECUTION PROCEDURE
-- =====================================================================
-- This procedure executes migration calls for additional MAP tables
-- in the STARCADPRODDATA schema that were not included in the main procedure
-- Generated on: $(date)
-- =====================================================================

USE ROLE r_dev_npd_d12_gdwmig;
USE DATABASE npd_d12_dmn_gdwmig;
USE SCHEMA migration_tracking;
USE WAREHOUSE wh_usr_npd_d12_gdwmig_001;

CREATE OR REPLACE PROCEDURE P_ADDITIONAL_MAP_TABLES_EXECUTE_MIGRATIONS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    result_msg STRING DEFAULT '';
    error_msg STRING DEFAULT '';
    call_count INT DEFAULT 0;
BEGIN
    result_msg := 'Starting Additional MAP Tables migration execution...\n';
   
    BEGIN
        -- Set context
        result_msg := result_msg || 'Context set successfully.\n';
       
        -- =====================================================================
        -- ADDITIONAL MAP TABLES MIGRATION CALLS
        -- =====================================================================
        
        -- Migration: MAP_CSE_ENV_PATY_REL
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_ENV_PATY_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_ENV_PATY_REL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_ENV_PATY_REL\n';

        -- Migration: MAP_CSE_LOAN_TERM_PL
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_LOAN_TERM_PL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_LOAN_TERM_PL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_LOAN_TERM_PL\n';

        -- Migration: MAP_CSE_FEAT_OVRD_REAS_HL
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_FEAT_OVRD_REAS_HL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_FEAT_OVRD_REAS_HL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_FEAT_OVRD_REAS_HL\n';

        -- Migration: MAP_CSE_FEAT_OVRD_REAS_PL
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_FEAT_OVRD_REAS_PL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_FEAT_OVRD_REAS_PL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_FEAT_OVRD_REAS_PL\n';

        -- Migration: MAP_CSE_FEAT_OVRD_REAS_HL_D
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_FEAT_OVRD_REAS_HL_D', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_FEAT_OVRD_REAS_HL_D', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_FEAT_OVRD_REAS_HL_D\n';

        -- Migration: MAP_CSE_OVRD_FEE_FRQ_CL
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_OVRD_FEE_FRQ_CL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_OVRD_FEE_FRQ_CL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_OVRD_FEE_FRQ_CL\n';

        -- Migration: MAP_CSE_ORIG_APPT_SRCE_HM
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_ORIG_APPT_SRCE_HM', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_ORIG_APPT_SRCE_HM', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_ORIG_APPT_SRCE_HM\n';

        -- Migration: MAP_CSE_JOB_COMM_CATG
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_JOB_COMM_CATG', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_JOB_COMM_CATG', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_JOB_COMM_CATG\n';

        -- Migration: MAP_CSE_PL_ACQR_TYPE
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_PL_ACQR_TYPE', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_PL_ACQR_TYPE', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_PL_ACQR_TYPE\n';

        -- Migration: MAP_CSE_PACK_PDCT_PL
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_PACK_PDCT_PL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_PACK_PDCT_PL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_PACK_PDCT_PL\n';

        -- Migration: MAP_CSE_PDCT_REL_CL
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_PDCT_REL_CL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_PDCT_REL_CL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_PDCT_REL_CL\n';

        -- Migration: MAP_CSE_PACK_PDCT_HL
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_PACK_PDCT_HL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_PACK_PDCT_HL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_PACK_PDCT_HL\n';

        -- Migration: MAP_CSE_STATE
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_STATE', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_STATE', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_STATE\n';

        -- Migration: MAP_CSE_ENV_PATY_TYPE
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_ENV_PATY_TYPE', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_ENV_PATY_TYPE', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_ENV_PATY_TYPE\n';

        -- Migration: MAP_CSE_PAYT_FREQ
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_PAYT_FREQ', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_PAYT_FREQ', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_PAYT_FREQ\n';

        -- Migration: MAP_CSE_ORIG_APPT_SRCE
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_ORIG_APPT_SRCE', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_ORIG_APPT_SRCE', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_ORIG_APPT_SRCE\n';

        -- Migration: MAP_CSE_LPC_DEPT_HL
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_LPC_DEPT_HL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_LPC_DEPT_HL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_LPC_DEPT_HL\n';

        -- Migration: MAP_CSE_LOAN_FNDD_METH
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_LOAN_FNDD_METH', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_LOAN_FNDD_METH', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_LOAN_FNDD_METH\n';

        -- Migration: MAP_CSE_FEE_CAPL
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_FEE_CAPL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_FEE_CAPL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_FEE_CAPL\n';

        -- Migration: MAP_CSE_ENV_EVNT_ACTV_TYPE
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_ENV_EVNT_ACTV_TYPE', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_ENV_EVNT_ACTV_TYPE', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_ENV_EVNT_ACTV_TYPE\n';

        -- Migration: MAP_CSE_ENV_CHLD_PATY_REL
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_ENV_CHLD_PATY_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_ENV_CHLD_PATY_REL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_ENV_CHLD_PATY_REL\n';

        -- Migration: MAP_CSE_TRNF_OPTN
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_TRNF_OPTN', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_TRNF_OPTN', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_TRNF_OPTN\n';

        -- Migration: MAP_CSE_FNDD_METH
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_FNDD_METH', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_FNDD_METH', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_FNDD_METH\n';

        -- Migration: MAP_CSE_UNID_PATY_CATG_PL
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_UNID_PATY_CATG_PL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_UNID_PATY_CATG_PL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_UNID_PATY_CATG_PL\n';

        -- Migration: MAP_CSE_TU_APPT_C
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_TU_APPT_C', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_TU_APPT_C', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_TU_APPT_C\n';

        -- =====================================================================
        -- COMPLETION
        -- =====================================================================
        
        result_msg := result_msg || '\n============================================\n';
        result_msg := result_msg || 'Additional MAP Tables Migration Summary:\n';
        result_msg := result_msg || 'Total migrations completed: ' || call_count || '\n';
        result_msg := result_msg || 'Schema: STARCADPRODDATA\n';
        result_msg := result_msg || 'Target Database: NPD_D12_DMN_GDWMIG_IBRG\n';
        result_msg := result_msg || '============================================\n';
        
        RETURN result_msg;
        
    EXCEPTION
        WHEN OTHER THEN
            error_msg := 'Error occurred during Additional MAP Tables migration: ' || SQLERRM;
            result_msg := result_msg || '\n' || error_msg;
            RETURN result_msg;
    END;
END;
$$;

-- =====================================================================
-- EXECUTION INSTRUCTIONS
-- =====================================================================
/*
To execute the Additional MAP Tables migration:

1. Ensure main STARCADPRODDATA migration has been completed first
2. Run this procedure:
   CALL P_ADDITIONAL_MAP_TABLES_EXECUTE_MIGRATIONS();

This will migrate all the additional MAP tables from the list:
- MAP_CSE_ENV_PATY_REL
- MAP_CSE_LOAN_TERM_PL
- MAP_CSE_FEAT_OVRD_REAS_HL
- MAP_CSE_FEAT_OVRD_REAS_PL
- MAP_CSE_FEAT_OVRD_REAS_HL_D
- MAP_CSE_OVRD_FEE_FRQ_CL
- MAP_CSE_ORIG_APPT_SRCE_HM
- MAP_CSE_JOB_COMM_CATG
- MAP_CSE_PL_ACQR_TYPE
- MAP_CSE_PACK_PDCT_PL
- MAP_CSE_PDCT_REL_CL
- MAP_CSE_PACK_PDCT_HL
- MAP_CSE_STATE
- MAP_CSE_ENV_PATY_TYPE
- MAP_CSE_PAYT_FREQ
- MAP_CSE_ORIG_APPT_SRCE
- MAP_CSE_LPC_DEPT_HL
- MAP_CSE_LOAN_FNDD_METH
- MAP_CSE_FEE_CAPL
- MAP_CSE_ENV_EVNT_ACTV_TYPE
- MAP_CSE_ENV_CHLD_PATY_REL
- MAP_CSE_TRNF_OPTN
- MAP_CSE_FNDD_METH
- MAP_CSE_UNID_PATY_CATG_PL
- MAP_CSE_TU_APPT_C

Total: 25 additional MAP tables
*/ 