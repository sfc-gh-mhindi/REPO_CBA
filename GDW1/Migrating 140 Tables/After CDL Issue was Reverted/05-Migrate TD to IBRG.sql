-- =====================================================================
-- WRAPPER PROCEDURE FOR MIGRATION WITH UPDATED SCHEMA/TABLE NAMES
-- =====================================================================
-- This procedure executes all migration calls with schema and table names
-- updated to match the format in 03-Creating Iceberg Tables.sql
-- (using CSV mapping for correct schema and table names)
-- =====================================================================

USE ROLE r_dev_npd_d12_gdwmig;
USE DATABASE NPD_D12_DMN_GDWMIG;
USE WAREHOUSE wh_usr_npd_d12_gdwmig_001;

CREATE OR REPLACE PROCEDURE EXECUTE_ALL_MIGRATIONS_UPDATED()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    result_msg STRING DEFAULT '';
    error_msg STRING DEFAULT '';
    call_count INT DEFAULT 0;
BEGIN
    result_msg := 'Starting migration execution with updated schema/table names...\n';
    
    BEGIN
        -- Set context
        USE ROLE r_dev_npd_d12_gdwmig;
        USE DATABASE npd_d12_dmn_gdwmig;
        USE SCHEMA migration_tracking;
        USE WAREHOUSE wh_usr_npd_d12_gdwmig_001;
        
        result_msg := result_msg || 'Context set successfully.\n';
        
        -- Migration 1
        CALL P_MIGRATE_TERADATA_TABLE('K_PDCBODS', 'ODS_RULE', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDCBODS', 'ODS_RULE', 'N', '', '', '', 'N');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 2
        CALL P_MIGRATE_TERADATA_TABLE('K_PDCMS', 'MAP_CMS_PDCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDCMS', 'MAP_CMS_PDCT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 3
        CALL P_MIGRATE_TERADATA_TABLE('K_PDGRD', 'DEPT_DIMN_NODE_ANCS_CURR', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDGRD', 'DEPT_DIMN_NODE_ANCS_CURR', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 4
        CALL P_MIGRATE_TERADATA_TABLE('K_PDGRD', 'GRD_DEPT_FLAT_CURR', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDGRD', 'GRD_DEPT_FLAT_CURR', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 5
        CALL P_MIGRATE_TERADATA_TABLE('K_PDGRD', 'GRD_GNRC_MAP', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDGRD', 'GRD_GNRC_MAP', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 6
        CALL P_MIGRATE_TERADATA_TABLE('K_PDGRD', 'NON_WORK_DAY', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDGRD', 'NON_WORK_DAY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 7
        CALL P_MIGRATE_TERADATA_TABLE('K_PDPATY', 'ACCT_PATY', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDPATY', 'ACCT_PATY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 8
        CALL P_MIGRATE_TERADATA_TABLE('K_PDPATY', 'PATY_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDPATY', 'PATY_REL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 9
        CALL P_MIGRATE_TERADATA_TABLE('K_PDPATY', 'UTIL_PROS_ISAC', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDPATY', 'UTIL_PROS_ISAC', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 10
        CALL P_MIGRATE_TERADATA_TABLE('K_PDSECURITY', 'ROW_LEVL_SECU_USER_PRFL', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDSECURITY', 'ROW_LEVL_SECU_USER_PRFL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 11
        CALL P_MIGRATE_TERADATA_TABLE('K_PDSRCCS', 'PLAN_BALN_SEGM_MSTR', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDSRCCS', 'PLAN_BALN_SEGM_MSTR', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 12
        CALL P_MIGRATE_TERADATA_TABLE('K_PDSRCCS', 'PLAN_BALN_SEGM_MSTR_ARCH', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDSRCCS', 'PLAN_BALN_SEGM_MSTR_ARCH', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 13
        CALL P_MIGRATE_TERADATA_TABLE('K_PDSRCCS', 'UTIL_BTCH_ISAC', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDSRCCS', 'UTIL_BTCH_ISAC', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 14
        CALL P_MIGRATE_TERADATA_TABLE('K_PDSRCCS', 'UTIL_PARM', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDSRCCS', 'UTIL_PARM', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 15
        CALL P_MIGRATE_TERADATA_TABLE('K_PDSRCCS', 'UTIL_PROS_ISAC', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDSRCCS', 'UTIL_PROS_ISAC', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 16
        CALL P_MIGRATE_TERADATA_TABLE('K_PDSRCCS', 'UTIL_TRSF_EROR_RQM3', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDSRCCS', 'UTIL_TRSF_EROR_RQM3', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 17
        CALL P_MIGRATE_TERADATA_TABLE('K_PDTRPC', 'WKND_PBLC_HLDY', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDTRPC', 'WKND_PBLC_HLDY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 18
        CALL P_MIGRATE_TERADATA_TABLE('K_PUTIL', 'TERASYNC', 'NPD_D12_DMN_GDWMIG_IBRG', 'PUTIL', 'TERASYNC', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 19
        CALL P_MIGRATE_TERADATA_TABLE('K_PUTIL', 'PROS_EROR_LOG', 'NPD_D12_DMN_GDWMIG_IBRG', 'PUTIL', 'PROS_EROR_LOG', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 20
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'ACCT_APPT_PDCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'ACCT_APPT_PDCT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 21
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'ACCT_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'ACCT_REL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 22
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'ACCT_UNID_PATY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'ACCT_UNID_PATY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 23
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'ACCT_XREF_BPS_CBS', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'ACCT_XREF_BPS_CBS', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 24
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'ACCT_XREF_MAS_DAR', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'ACCT_XREF_MAS_DAR', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 25
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 26
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_DEPT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_DEPT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 27
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_FEAT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_FEAT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 28
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_PDCT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 29
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_ACCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_PDCT_ACCT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 30
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_AMT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_PDCT_AMT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 31
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_FEAT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_PDCT_FEAT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 32
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_PATY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_PDCT_PATY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 33
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_PURP', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_PDCT_PURP', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 34
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_PDCT_REL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 35
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_RPAY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_PDCT_RPAY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 36
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_PDCT_UNID_PATY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_PDCT_UNID_PATY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 37
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_REL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 38
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'APPT_TRNF_DETL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'APPT_TRNF_DETL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 39
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'BUSN_EVNT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'BUSN_EVNT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 40
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'CLS_FCLY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'CLS_FCLY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 41
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'CLS_UNID_PATY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'CLS_UNID_PATY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 42
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'DAR_ACCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'DAR_ACCT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 43
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'DEPT_APPT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'DEPT_APPT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 44
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'DERV_ACCT_PATY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'DERV_ACCT_PATY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 45
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'DERV_PRTF_ACCT_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'DERV_PRTF_ACCT_REL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 46
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'DERV_PRTF_OWN_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'DERV_PRTF_OWN_REL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 47
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'DERV_PRTF_PATY_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'DERV_PRTF_PATY_REL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 48
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'EVNT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'EVNT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 49
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'EVNT_EMPL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'EVNT_EMPL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 50
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'EVNT_INT_GRUP', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'EVNT_INT_GRUP', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 51
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'GDW_EFFT_DATE', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'GDW_EFFT_DATE', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 52
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'INT_GRUP', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'INT_GRUP', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 53
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'INT_GRUP_DEPT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'INT_GRUP_DEPT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 54
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'INT_GRUP_EMPL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'INT_GRUP_EMPL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 55
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'INT_GRUP_UNID_PATY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'INT_GRUP_UNID_PATY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 56
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_ADRS_TYPE', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_ADRS_TYPE', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 57
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_ACQR_SRCE', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_ACQR_SRCE', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 58
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_C', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_C', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 59
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_CMPE', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_CMPE', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 60
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_CODE_HM', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_CODE_HM', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 61
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_COND', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_COND', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 62
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_DOCU_DELY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_DOCU_DELY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 63
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_FEAT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_FEAT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 64
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_FORM', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_FORM', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 65
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_ORIG', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_ORIG', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 66
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_PDCT_FEAT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_PDCT_FEAT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 67
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_PDCT_PATY_ROLE', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_PDCT_PATY_ROLE', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 68
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_PURP_CL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_PURP_CL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 69
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_PURP_CLAS_CL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_PURP_CLAS_CL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 70
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_PURP_HL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_PURP_HL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 71
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_PURP_PL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_PURP_PL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 72
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_QLFY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_QLFY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 73
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_QSTN_HL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_QSTN_HL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 74
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_APPT_QSTN_RESP_HL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_APPT_QSTN_RESP_HL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 75
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_CMPE_IDNN', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_CMPE_IDNN', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 76
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_CNTY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_CNTY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 77
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_CRIS_PDCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_CRIS_PDCT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 78
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_DOCU_METH', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_DOCU_METH', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 79
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_ENV_CHLD_PATY_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_ENV_CHLD_PATY_REL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 80
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_ENV_EVNT_ACTV_TYPE', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_ENV_EVNT_ACTV_TYPE', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 81
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_ENV_PATY_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_ENV_PATY_REL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 82
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_ENV_PATY_TYPE', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_ENV_PATY_TYPE', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 83
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_FEAT_OVRD_REAS_HL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_FEAT_OVRD_REAS_HL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 84
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_FEAT_OVRD_REAS_HL_D', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_FEAT_OVRD_REAS_HL_D', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 85
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_FEAT_OVRD_REAS_PL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_FEAT_OVRD_REAS_PL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 86
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_FEE_CAPL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_FEE_CAPL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 87
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_FNDD_METH', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_FNDD_METH', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 88
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_JOB_COMM_CATG', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_JOB_COMM_CATG', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 89
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_LOAN_FNDD_METH', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_LOAN_FNDD_METH', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 90
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_LOAN_TERM_PL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_LOAN_TERM_PL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 91
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_LPC_DEPT_HL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_LPC_DEPT_HL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 92
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_ORIG_APPT_SRCE', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_ORIG_APPT_SRCE', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 93
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_ORIG_APPT_SRCE_HM', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_ORIG_APPT_SRCE_HM', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 94
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_OVRD_FEE_FRQ_CL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_OVRD_FEE_FRQ_CL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 95
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_PACK_PDCT_HL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_PACK_PDCT_HL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 96
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_PACK_PDCT_PL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_PACK_PDCT_PL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 97
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_PAYT_FREQ', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_PAYT_FREQ', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 98
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_PDCT_REL_CL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_PDCT_REL_CL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 99
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_PL_ACQR_TYPE', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_PL_ACQR_TYPE', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 100
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_SM_CASE_STUS', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_SM_CASE_STUS', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 101
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_SM_CASE_STUS_REAS', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_SM_CASE_STUS_REAS', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 102
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_STATE', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_STATE', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 103
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_TRNF_OPTN', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_TRNF_OPTN', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 104
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_TU_APPT_C', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_TU_APPT_C', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 105
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_CSE_UNID_PATY_CATG_PL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_CSE_UNID_PATY_CATG_PL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 106
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MOS_FCLY', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MOS_FCLY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 107
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MOS_LOAN', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MOS_LOAN', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 108
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'PATY_APPT_PDCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'PATY_APPT_PDCT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 109
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'PATY_INT_GRUP', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'PATY_INT_GRUP', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 110
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'THA_ACCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'THA_ACCT', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 111
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'UTIL_BTCH_ISAC', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'UTIL_BTCH_ISAC', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 112
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'UTIL_ETI_CONV', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'UTIL_ETI_CONV', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 113
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'UTIL_PARM', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'UTIL_PARM', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 114
        CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'UTIL_PROS_ISAC', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'UTIL_PROS_ISAC', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 115
        CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_ACCT_PATY_NON_RM', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDDSTG', 'DERV_ACCT_PATY_NON_RM', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 116
        CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'ACCT_PATY_THA_NEW_RNGE', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDDSTG', 'ACCT_PATY_THA_NEW_RNGE', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 117
        CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'ACCT_PATY_REL_WSS', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDDSTG', 'ACCT_PATY_REL_WSS', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 118
        CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_PRTF_PATY_STAG', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDDSTG', 'DERV_PRTF_PATY_STAG', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 119
        CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'GRD_GNRC_MAP_DERV_PATY_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDDSTG', 'GRD_GNRC_MAP_DERV_PATY_REL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 120
        CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_ACCT_PATY_ROW_SECU_FIX', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDDSTG', 'DERV_ACCT_PATY_ROW_SECU_FIX', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 121
        CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_ACCT_PATY_FLAG', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDDSTG', 'DERV_ACCT_PATY_FLAG', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 122
        CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'ACCT_PATY_DEDUP', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDDSTG', 'ACCT_PATY_DEDUP', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 123
        CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'ACCT_REL_WSS_DITPS', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDDSTG', 'ACCT_REL_WSS_DITPS', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 124
        CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'GRD_GNRC_MAP_PATY_HOLD_PRTY', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDDSTG', 'GRD_GNRC_MAP_PATY_HOLD_PRTY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 125
        CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_PRTF_ACCT_STAG', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDDSTG', 'DERV_PRTF_ACCT_STAG', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 126
        CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'ACCT_PATY_REL_THA', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDDSTG', 'ACCT_PATY_REL_THA', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 127
        CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'GRD_GNRC_MAP_DERV_PATY_HOLD', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDDSTG', 'GRD_GNRC_MAP_DERV_PATY_HOLD', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 128
        CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'GRD_GNRC_MAP_BUSN_SEGM_PRTY', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDDSTG', 'GRD_GNRC_MAP_BUSN_SEGM_PRTY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 129
        CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_PRTF_ACCT_PATY_PSST', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDDSTG', 'DERV_PRTF_ACCT_PATY_PSST', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 130
        CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_ACCT_PATY_CURR', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDDSTG', 'DERV_ACCT_PATY_CURR', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 131
        CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'GRD_GNRC_MAP_DERV_UNID_PATY', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDDSTG', 'GRD_GNRC_MAP_DERV_UNID_PATY', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 132
        CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_ACCT_PATY_DEL', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDDSTG', 'DERV_ACCT_PATY_DEL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 133
        CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_ACCT_PATY_CHG', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDDSTG', 'DERV_ACCT_PATY_CHG', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 134
        CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_PRTF_ACCT_PATY_STAG', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDDSTG', 'DERV_PRTF_ACCT_PATY_STAG', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 135
        CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_ACCT_PATY_RM', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDDSTG', 'DERV_ACCT_PATY_RM', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 136
        CALL P_MIGRATE_TERADATA_TABLE('K_PDDSTG', 'DERV_ACCT_PATY_ADD', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDDSTG', 'DERV_ACCT_PATY_ADD', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 137
        CALL P_MIGRATE_TERADATA_TABLE('Sys_Calendar', 'CALDATES', 'NPD_D12_DMN_GDWMIG_IBRG', 'Sys_Calendar', 'CALDATES', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        -- Migration 138
        CALL P_MIGRATE_TERADATA_TABLE('SysAdmin', 'TPUMPSTATUSTBL', 'NPD_D12_DMN_GDWMIG_IBRG', 'SysAdmin', 'TPUMPSTATUSTBL', 'N', '', '', '', 'Y');
        call_count := call_count + 1;
        result_msg := result_msg || 'Completed migration ' || call_count || '\n';
        
        result_msg := result_msg || 'All ' || call_count || ' migrations completed successfully.';
        
    EXCEPTION
        WHEN OTHER THEN
            error_msg := 'Error during migration execution: ' || SQLERRM;
            result_msg := result_msg || error_msg;
            RETURN result_msg;
    END;
    
    RETURN result_msg;
END;
$$;

-- Example usage:
-- CALL EXECUTE_ALL_MIGRATIONS_UPDATED();
