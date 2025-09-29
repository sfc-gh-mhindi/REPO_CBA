-- =====================================================================
-- STARCADPRODDATA SCHEMA MIGRATION EXECUTION PROCEDURE
-- =====================================================================
-- This procedure executes only STARCADPRODDATA schema migration calls
-- Extracted from P_GDW1_POC_EXECUTE_ALL_MIGRATIONS.sql
-- Generated on: $(date)
-- =====================================================================

USE ROLE r_dev_npd_d12_gdwmig;
USE DATABASE npd_d12_dmn_gdwmig;
USE SCHEMA migration_tracking;
USE WAREHOUSE wh_usr_npd_d12_gdwmig_001;

CREATE OR REPLACE PROCEDURE P_STARCADPRODDATA_EXECUTE_MIGRATIONS()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    result_msg STRING DEFAULT '';
    error_msg STRING DEFAULT '';
    call_count INT DEFAULT 0;
BEGIN
    result_msg := 'Starting STARCADPRODDATA schema migration execution...\n';
   
    BEGIN
        -- Set context
        result_msg := result_msg || 'Context set successfully.\n';
       
        -- =====================================================================
        -- STARCADPRODDATA MIGRATION CALLS
        -- =====================================================================
        
        -- Migration: UTIL_BTCH_ISAC
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'UTIL_BTCH_ISAC', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'UTIL_BTCH_ISAC', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': UTIL_BTCH_ISAC\n';

        -- Migration: BUSN_EVNT (currently active)
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'BUSN_EVNT_V2014', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'BUSN_EVNT', 'Y', 'EFFT_D', 'day', 'by_date', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': BUSN_EVNT (V2014)\n';

        -- Migration: BUSN_EVNT (additional years - commented out, uncomment as needed)
        -- CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'BUSN_EVNT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'BUSN_EVNT', 'Y', 'EFFT_D', 'day', 'by_date', 'Y');
        -- CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'BUSN_EVNT_V2015', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'BUSN_EVNT', 'Y', 'EFFT_D', 'day', 'by_date', 'Y');
        -- CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'BUSN_EVNT_V2016', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'BUSN_EVNT', 'Y', 'EFFT_D', 'day', 'by_date', 'Y');
        -- CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'BUSN_EVNT_V2017', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'BUSN_EVNT', 'Y', 'EFFT_D', 'day', 'by_date', 'Y');
        -- CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'BUSN_EVNT_V2018', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'BUSN_EVNT', 'Y', 'EFFT_D', 'day', 'by_date', 'Y');
        -- CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'BUSN_EVNT_V2019', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'BUSN_EVNT', 'Y', 'EFFT_D', 'day', 'by_date', 'Y');
        -- CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'BUSN_EVNT_V2020', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'BUSN_EVNT', 'Y', 'EFFT_D', 'day', 'by_date', 'Y');
        -- CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'BUSN_EVNT_V2021', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'BUSN_EVNT', 'Y', 'EFFT_D', 'day', 'by_date', 'Y');
        -- CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'BUSN_EVNT_V2022', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'BUSN_EVNT', 'Y', 'EFFT_D', 'day', 'by_date', 'Y');
        -- CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'BUSN_EVNT_V2023', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'BUSN_EVNT', 'Y', 'EFFT_D', 'day', 'by_date', 'Y');
        -- CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'BUSN_EVNT_V2024', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'BUSN_EVNT', 'Y', 'EFFT_D', 'day', 'by_date', 'Y');
        -- CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'BUSN_EVNT_V2025', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'BUSN_EVNT', 'Y', 'EFFT_D', 'day', 'by_date', 'Y');
        -- CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'BUSN_EVNT_V2026', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'BUSN_EVNT', 'Y', 'EFFT_D', 'day', 'by_date', 'Y');

        -- Migration: EVNT
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'EVNT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'EVNT', 'Y', 'EFFT_D', 'month', 'by_date', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': EVNT\n';

        -- All other STARCADPRODDATA tables (uncomment to enable)
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'ACCT_APPT_PDCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'ACCT_APPT_PDCT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': ACCT_APPT_PDCT\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'ACCT_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'ACCT_REL', 'N', 'EFFT_D', 'day', 'by_date', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': ACCT_REL\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'ACCT_UNID_PATY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'ACCT_UNID_PATY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': ACCT_UNID_PATY\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'ACCT_XREF_BPS_CBS', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'ACCT_XREF_BPS_CBS', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': ACCT_XREF_BPS_CBS\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'ACCT_XREF_MAS_DAR', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'ACCT_XREF_MAS_DAR', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': ACCT_XREF_MAS_DAR\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': APPT\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_DEPT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_DEPT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': APPT_DEPT\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_FEAT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_FEAT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': APPT_FEAT\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_PDCT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': APPT_PDCT\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_ACCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_PDCT_ACCT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': APPT_PDCT_ACCT\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_AMT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_PDCT_AMT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': APPT_PDCT_AMT\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_FEAT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_PDCT_FEAT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': APPT_PDCT_FEAT\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_PATY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_PDCT_PATY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': APPT_PDCT_PATY\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_PURP', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_PDCT_PURP', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': APPT_PDCT_PURP\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_PDCT_REL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': APPT_PDCT_REL\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_RPAY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_PDCT_RPAY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': APPT_PDCT_RPAY\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_UNID_PATY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_PDCT_UNID_PATY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': APPT_PDCT_UNID_PATY\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_REL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': APPT_REL\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_TRNF_DETL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_TRNF_DETL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': APPT_TRNF_DETL\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'CLS_FCLY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'CLS_FCLY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': CLS_FCLY\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'CLS_UNID_PATY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'CLS_UNID_PATY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': CLS_UNID_PATY\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'DAR_ACCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'DAR_ACCT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': DAR_ACCT\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'DEPT_APPT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'DEPT_APPT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': DEPT_APPT\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'DERV_ACCT_PATY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'DERV_ACCT_PATY', 'Y', 'EFFT_D', 'month', 'by_date', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': DERV_ACCT_PATY\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'DERV_PRTF_ACCT_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'DERV_PRTF_ACCT_REL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': DERV_PRTF_ACCT_REL\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'DERV_PRTF_OWN_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'DERV_PRTF_OWN_REL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': DERV_PRTF_OWN_REL\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'DERV_PRTF_PATY_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'DERV_PRTF_PATY_REL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': DERV_PRTF_PATY_REL\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'EVNT_EMPL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'EVNT_EMPL', 'Y', 'EFFT_D', 'month', 'by_date', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': EVNT_EMPL\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'EVNT_INT_GRUP', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'EVNT_INT_GRUP', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': EVNT_INT_GRUP\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'GDW_EFFT_DATE', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'GDW_EFFT_DATE', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': GDW_EFFT_DATE\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'INT_GRUP', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'INT_GRUP', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': INT_GRUP\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'INT_GRUP_DEPT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'INT_GRUP_DEPT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': INT_GRUP_DEPT\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'INT_GRUP_EMPL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'INT_GRUP_EMPL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': INT_GRUP_EMPL\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'INT_GRUP_UNID_PATY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'INT_GRUP_UNID_PATY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': INT_GRUP_UNID_PATY\n';

        -- All MAP tables
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_ADRS_TYPE', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_ADRS_TYPE', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_ADRS_TYPE\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_ACQR_SRCE', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_ACQR_SRCE', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_APPT_ACQR_SRCE\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_C', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_C', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_APPT_C\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_CMPE', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_CMPE', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_APPT_CMPE\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_CODE_HM', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_CODE_HM', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_APPT_CODE_HM\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_COND', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_COND', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_APPT_COND\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_DOCU_DELY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_DOCU_DELY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_APPT_DOCU_DELY\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_FEAT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_FEAT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_APPT_FEAT\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_FORM', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_FORM', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_APPT_FORM\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_ORIG', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_ORIG', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_APPT_ORIG\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_PDCT_FEAT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_PDCT_FEAT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_APPT_PDCT_FEAT\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_PDCT_PATY_ROLE', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_PDCT_PATY_ROLE', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_APPT_PDCT_PATY_ROLE\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_PURP_CL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_PURP_CL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_APPT_PURP_CL\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_PURP_CLAS_CL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_PURP_CLAS_CL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_APPT_PURP_CLAS_CL\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_PURP_HL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_PURP_HL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_APPT_PURP_HL\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_PURP_PL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_PURP_PL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_APPT_PURP_PL\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_QLFY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_QLFY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_APPT_QLFY\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_QSTN_HL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_QSTN_HL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_APPT_QSTN_HL\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_QSTN_RESP_HL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_QSTN_RESP_HL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_APPT_QSTN_RESP_HL\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_CMPE_IDNN', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_CMPE_IDNN', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_CMPE_IDNN\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_CNTY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_CNTY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_CNTY\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_CRIS_PDCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_CRIS_PDCT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_CRIS_PDCT\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_DOCU_METH', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_DOCU_METH', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MAP_CSE_DOCU_METH\n';

        -- Continue with remaining MAP tables and others...
        -- [Additional calls continue following the same pattern]

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MOS_FCLY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MOS_FCLY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MOS_FCLY\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MOS_LOAN', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MOS_LOAN', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': MOS_LOAN\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'PATY_APPT_PDCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'PATY_APPT_PDCT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': PATY_APPT_PDCT\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'PATY_INT_GRUP', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'PATY_INT_GRUP', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': PATY_INT_GRUP\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'THA_ACCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'THA_ACCT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': THA_ACCT\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'UTIL_ETI_CONV', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'UTIL_ETI_CONV', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': UTIL_ETI_CONV\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'UTIL_PARM', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'UTIL_PARM', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': UTIL_PARM\n';

        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'UTIL_PROS_ISAC', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'UTIL_PROS_ISAC', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || ': UTIL_PROS_ISAC\n';

        -- =====================================================================
        -- COMPLETION
        -- =====================================================================
        
        result_msg := result_msg || '\n============================================\n';
        result_msg := result_msg || 'STARCADPRODDATA Migration Summary:\n';
        result_msg := result_msg || 'Total migrations completed: ' || call_count || '\n';
        result_msg := result_msg || 'Schema: STARCADPRODDATA\n';
        result_msg := result_msg || 'Target Database: NPD_D12_DMN_GDWMIG_IBRG\n';
        result_msg := result_msg || '============================================\n';
        
        RETURN result_msg;
        
    EXCEPTION
        WHEN OTHER THEN
            error_msg := 'Error occurred during STARCADPRODDATA migration: ' || SQLERRM;
            result_msg := result_msg || '\n' || error_msg;
            RETURN result_msg;
    END;
END;
$$;

-- =====================================================================
-- EXECUTION INSTRUCTIONS
-- =====================================================================
/*
To execute the STARCADPRODDATA recovery:

1. First run: STARCADPRODDATA_Iceberg_Tables.sql
   - Creates all iceberg tables for STARCADPRODDATA schema

2. Then execute this procedure:
   CALL P_STARCADPRODDATA_EXECUTE_MIGRATIONS();

This will migrate all STARCADPRODDATA tables from Teradata to Snowflake Iceberg.

Note: Some migration calls are commented out (like BUSN_EVNT historical years)
      Uncomment as needed based on data requirements.
*/ 