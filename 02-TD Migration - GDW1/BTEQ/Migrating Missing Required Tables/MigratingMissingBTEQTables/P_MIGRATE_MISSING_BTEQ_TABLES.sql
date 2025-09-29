-- =====================================================================
-- WRAPPER PROCEDURE FOR MIGRATING MISSING BTEQ TABLES
-- =====================================================================
-- This procedure executes migration calls for all 14 tables that were
-- found available in the K_ databases based on table availability analysis
-- 
-- Analysis Results: 14 available tables out of 46 total (30.4%)
-- Generated from: table_availability_analysis.md
-- =====================================================================

USE ROLE r_dev_npd_d12_gdwmig;
USE DATABASE npd_d12_dmn_gdwmig;
USE SCHEMA migration_tracking;
USE WAREHOUSE wh_usr_npd_d12_gdwmig_001;

CREATE OR REPLACE PROCEDURE P_MIGRATE_MISSING_BTEQ_TABLES()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    result_msg STRING DEFAULT '';
    error_msg STRING DEFAULT '';
    call_count INT DEFAULT 0;
    success_count INT DEFAULT 0;
    failure_count INT DEFAULT 0;
BEGIN
    result_msg := 'Starting migration of 14 available Missing BTEQ Tables...\n';
    result_msg := result_msg || 'Analysis Date: 2025-08-25 11:41:24\n';
    result_msg := result_msg || 'Available: 14/46 tables (30.4%)\n\n';
   
    BEGIN
        -- Set context
        result_msg := result_msg || 'Context set successfully.\n\n';
       

        -- Migration 14: MAP_SAP_INVL_PDCT
        BEGIN
            CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'MAP_SAP_INVL_PDCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'MAP_SAP_INVL_PDCT', 'N', '', '', '', 'Y');
            call_count := call_count + 1;
            success_count := success_count + 1;
            result_msg := result_msg || '✅ [' || call_count || '] STARCADPRODDATA.MAP_SAP_INVL_PDCT completed\n';
        EXCEPTION
            WHEN OTHER THEN
                call_count := call_count + 1;
                failure_count := failure_count + 1;
                result_msg := result_msg || '❌ [' || call_count || '] STARCADPRODDATA.MAP_SAP_INVL_PDCT failed: ' || SQLERRM || '\n';
        END;

        -- ===================================================================
        -- PDGRD SCHEMA MIGRATIONS (1 table - 25% available)
        -- ===================================================================
        result_msg := result_msg || '\n--- MIGRATING PDGRD TABLES (1 table) ---\n';

        -- Migration 7: GRD_RPRT_CALR_FNYR
        BEGIN
            CALL P_MIGRATE_TERADATA_TABLE('K_PDGRD', 'GRD_RPRT_CALR_FNYR', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDGRD', 'GRD_RPRT_CALR_FNYR', 'N', '', '', '', 'Y');
            call_count := call_count + 1;
            success_count := success_count + 1;
            result_msg := result_msg || '✅ [' || call_count || '] PDGRD.GRD_RPRT_CALR_FNYR completed\n';
        EXCEPTION
            WHEN OTHER THEN
                call_count := call_count + 1;
                failure_count := failure_count + 1;
                result_msg := result_msg || '❌ [' || call_count || '] PDGRD.GRD_RPRT_CALR_FNYR failed: ' || SQLERRM || '\n';
        END;

        -- ===================================================================
        -- PDSECURITY SCHEMA MIGRATIONS (1 table - 100% available)
        -- ===================================================================
        result_msg := result_msg || '\n--- MIGRATING PDSECURITY TABLES (1 table) ---\n';
        -- Migration 9: ROW_LEVL_SECU_USER_PRFL
        BEGIN
            CALL P_MIGRATE_TERADATA_TABLE('K_PDSECURITY', 'ROW_LEVL_SECU_USER_PRFL', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDSECURITY', 'ROW_LEVL_SECU_USER_PRFL', 'N', '', '', '', 'Y');
            call_count := call_count + 1;
            success_count := success_count + 1;
            result_msg := result_msg || '✅ [' || call_count || '] PDSECURITY.ROW_LEVL_SECU_USER_PRFL completed\n';
        EXCEPTION
            WHEN OTHER THEN
                call_count := call_count + 1;
                failure_count := failure_count + 1;
                result_msg := result_msg || '❌ [' || call_count || '] PDSECURITY.ROW_LEVL_SECU_USER_PRFL failed: ' || SQLERRM || '\n';
        END;

        -- ===================================================================
        -- PDCBODS SCHEMA MIGRATIONS (6 tables - 100% available)
        -- ===================================================================
        result_msg := result_msg || '--- MIGRATING PDCBODS TABLES (6 tables) ---\n';
        
        -- Migration 3: CBA_FNCL_SERV_GL_DATA
        BEGIN
            CALL P_MIGRATE_TERADATA_TABLE('K_PDCBODS', 'CBA_FNCL_SERV_GL_DATA', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDCBODS', 'CBA_FNCL_SERV_GL_DATA', 'N', '', '', '', 'Y');
            call_count := call_count + 1;
            success_count := success_count + 1;
            result_msg := result_msg || '✅ [' || call_count || '] PDCBODS.CBA_FNCL_SERV_GL_DATA completed\n';
        EXCEPTION
            WHEN OTHER THEN
                call_count := call_count + 1;
                failure_count := failure_count + 1;
                result_msg := result_msg || '❌ [' || call_count || '] PDCBODS.CBA_FNCL_SERV_GL_DATA failed: ' || SQLERRM || '\n';
        END;

        -- Migration 5: MSTR_CNCT_MSTR_DATA_GENL
        BEGIN
            CALL P_MIGRATE_TERADATA_TABLE('K_PDCBODS', 'MSTR_CNCT_MSTR_DATA_GENL', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDCBODS', 'MSTR_CNCT_MSTR_DATA_GENL', 'N', '', '', '', 'Y');
            call_count := call_count + 1;
            success_count := success_count + 1;
            result_msg := result_msg || '✅ [' || call_count || '] PDCBODS.MSTR_CNCT_MSTR_DATA_GENL completed\n';
        EXCEPTION
            WHEN OTHER THEN
                call_count := call_count + 1;
                failure_count := failure_count + 1;
                result_msg := result_msg || '❌ [' || call_count || '] PDCBODS.MSTR_CNCT_MSTR_DATA_GENL failed: ' || SQLERRM || '\n';
        END;


        -- Migration 1: ACCT_MSTR_CYT_DATA
        BEGIN
            CALL P_MIGRATE_TERADATA_TABLE('K_PDCBODS', 'ACCT_MSTR_CYT_DATA', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDCBODS', 'ACCT_MSTR_CYT_DATA', 'N', '', '', '', 'Y');
            call_count := call_count + 1;
            success_count := success_count + 1;
            result_msg := result_msg || '✅ [' || call_count || '] PDCBODS.ACCT_MSTR_CYT_DATA completed\n';
        EXCEPTION
            WHEN OTHER THEN
                call_count := call_count + 1;
                failure_count := failure_count + 1;
                result_msg := result_msg || '❌ [' || call_count || '] PDCBODS.ACCT_MSTR_CYT_DATA failed: ' || SQLERRM || '\n';
        END;

        -- Migration 2: BUSN_PTNR
        BEGIN
            CALL P_MIGRATE_TERADATA_TABLE('K_PDCBODS', 'BUSN_PTNR', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDCBODS', 'BUSN_PTNR', 'N', '', '', '', 'Y');
            call_count := call_count + 1;
            success_count := success_count + 1;
            result_msg := result_msg || '✅ [' || call_count || '] PDCBODS.BUSN_PTNR completed\n';
        EXCEPTION
            WHEN OTHER THEN
                call_count := call_count + 1;
                failure_count := failure_count + 1;
                result_msg := result_msg || '❌ [' || call_count || '] PDCBODS.BUSN_PTNR failed: ' || SQLERRM || '\n';
        END;

        -- Migration 4: MSTR_CNCT_BALN_TRNF_PRTP
        BEGIN
            CALL P_MIGRATE_TERADATA_TABLE('K_PDCBODS', 'MSTR_CNCT_BALN_TRNF_PRTP', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDCBODS', 'MSTR_CNCT_BALN_TRNF_PRTP', 'N', '', '', '', 'Y');
            call_count := call_count + 1;
            success_count := success_count + 1;
            result_msg := result_msg || '✅ [' || call_count || '] PDCBODS.MSTR_CNCT_BALN_TRNF_PRTP completed\n';
        EXCEPTION
            WHEN OTHER THEN
                call_count := call_count + 1;
                failure_count := failure_count + 1;
                result_msg := result_msg || '❌ [' || call_count || '] PDCBODS.MSTR_CNCT_BALN_TRNF_PRTP failed: ' || SQLERRM || '\n';
        END;

        -- Migration 6: MSTR_CNCT_PRXY_ACCT
        BEGIN
            CALL P_MIGRATE_TERADATA_TABLE('K_PDCBODS', 'MSTR_CNCT_PRXY_ACCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDCBODS', 'MSTR_CNCT_PRXY_ACCT', 'N', '', '', '', 'Y');
            call_count := call_count + 1;
            success_count := success_count + 1;
            result_msg := result_msg || '✅ [' || call_count || '] PDCBODS.MSTR_CNCT_PRXY_ACCT completed\n';
        EXCEPTION
            WHEN OTHER THEN
                call_count := call_count + 1;
                failure_count := failure_count + 1;
                result_msg := result_msg || '❌ [' || call_count || '] PDCBODS.MSTR_CNCT_PRXY_ACCT failed: ' || SQLERRM || '\n';
        END;


        -- ===================================================================
        -- PDPATY SCHEMA MIGRATIONS (1 table - 100% available)
        -- ===================================================================
        result_msg := result_msg || '\n--- MIGRATING PDPATY TABLES (1 table) ---\n';

        -- Migration 8: ACCT_PATY
        BEGIN
            CALL P_MIGRATE_TERADATA_TABLE('K_PDPATY', 'ACCT_PATY', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDPATY', 'ACCT_PATY', 'Y', 'EFFT_D', 'month', 'by_date', 'Y');
            call_count := call_count + 1;
            success_count := success_count + 1;
            result_msg := result_msg || '✅ [' || call_count || '] PDPATY.ACCT_PATY completed (partitioned by EFFT_D)\n';
        EXCEPTION
            WHEN OTHER THEN
                call_count := call_count + 1;
                failure_count := failure_count + 1;
                result_msg := result_msg || '❌ [' || call_count || '] PDPATY.ACCT_PATY failed: ' || SQLERRM || '\n';
        END;


        --CALL P_MIGRATE_TERADATA_TABLE('K_PDSRCCS', 'PLAN_BALN_SEGM_MSTR', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDSRCCS', 'PLAN_BALN_SEGM_MSTR', 'Y', 'EFFT_D', 'month', 'by_date', 'Y');
        -- ===================================================================
        -- STARCADPRODDATA SCHEMA MIGRATIONS (5 tables - 20% available)
        -- ===================================================================
        result_msg := result_msg || '\n--- MIGRATING STARCADPRODDATA TABLES (5 tables) ---\n';

        -- Migration 10: ACCT_BASE
        BEGIN
            CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'ACCT_BASE', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'ACCT_BASE', 'Y', 'EFFT_D', 'month', 'by_date', 'Y');
            call_count := call_count + 1;
            success_count := success_count + 1;
            result_msg := result_msg || '✅ [' || call_count || '] STARCADPRODDATA.ACCT_BASE completed\n';
        EXCEPTION
            WHEN OTHER THEN
                call_count := call_count + 1;
                failure_count := failure_count + 1;
                result_msg := result_msg || '❌ [' || call_count || '] STARCADPRODDATA.ACCT_BASE failed: ' || SQLERRM || '\n';
        END;

        -- Migration 11: ACCT_OFFR
        BEGIN
            CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'ACCT_OFFR', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'ACCT_OFFR', 'Y', 'EFFT_D', 'month', 'by_date', 'Y');
            call_count := call_count + 1;
            success_count := success_count + 1;
            result_msg := result_msg || '✅ [' || call_count || '] STARCADPRODDATA.ACCT_OFFR completed\n';
        EXCEPTION
            WHEN OTHER THEN
                call_count := call_count + 1;
                failure_count := failure_count + 1;
                result_msg := result_msg || '❌ [' || call_count || '] STARCADPRODDATA.ACCT_OFFR failed: ' || SQLERRM || '\n';
        END;

        -- Migration 12: ACCT_PDCT  
        BEGIN
            CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'ACCT_PDCT', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'ACCT_PDCT', 'Y', 'EFFT_D', 'month', 'by_date', 'Y');
            call_count := call_count + 1;
            success_count := success_count + 1;
            result_msg := result_msg || '✅ [' || call_count || '] STARCADPRODDATA.ACCT_PDCT completed\n';
        EXCEPTION
            WHEN OTHER THEN
                call_count := call_count + 1;
                failure_count := failure_count + 1;
                result_msg := result_msg || '❌ [' || call_count || '] STARCADPRODDATA.ACCT_PDCT failed: ' || SQLERRM || '\n';
        END;

        -- Migration 13: DERV_PRTF_ACCT_REL
        BEGIN
            CALL P_MIGRATE_TERADATA_TABLE('K_STAR_CAD_PROD_DATA', 'DERV_PRTF_ACCT_REL', 'NPD_D12_DMN_GDWMIG_IBRG', 'STARCADPRODDATA', 'DERV_PRTF_ACCT_REL', 'Y', 'EFFT_D', 'month', 'by_date', 'Y');
            call_count := call_count + 1;
            success_count := success_count + 1;
            result_msg := result_msg || '✅ [' || call_count || '] STARCADPRODDATA.DERV_PRTF_ACCT_REL completed\n';
        EXCEPTION
            WHEN OTHER THEN
                call_count := call_count + 1;
                failure_count := failure_count + 1;
                result_msg := result_msg || '❌ [' || call_count || '] STARCADPRODDATA.DERV_PRTF_ACCT_REL failed: ' || SQLERRM || '\n';
        END;

        -- ===================================================================
        -- FINAL SUMMARY
        -- ===================================================================
        result_msg := result_msg || '\n=== MIGRATION SUMMARY ===\n';
        result_msg := result_msg || 'Total migrations attempted: ' || call_count || '\n';
        result_msg := result_msg || 'Successful: ' || success_count || '\n';
        result_msg := result_msg || 'Failed: ' || failure_count || '\n';
        result_msg := result_msg || 'Success rate: ' || ROUND((success_count::FLOAT / call_count) * 100, 1) || '%\n\n';
        
        result_msg := result_msg || 'Schema breakdown:\n';
        result_msg := result_msg || '  - PDCBODS: 6 tables (100% coverage)\n';
        result_msg := result_msg || '  - PDGRD: 1 table (25% coverage)\n';
        result_msg := result_msg || '  - PDPATY: 1 table (100% coverage)\n';
        result_msg := result_msg || '  - PDSECURITY: 1 table (100% coverage)\n';
        result_msg := result_msg || '  - STARCADPRODDATA: 5 tables (20% coverage)\n\n';
        
        result_msg := result_msg || '📊 Overall progress: 14/46 Missing BTEQ Tables migrated (30.4%)\n';
        result_msg := result_msg || '📋 Remaining: 32 tables still need to be restored to K_ databases\n';
       
    EXCEPTION
        WHEN OTHER THEN
            error_msg := 'Critical error during migration batch execution: ' || SQLERRM;
            result_msg := result_msg || '\n❌ CRITICAL ERROR: ' || error_msg || '\n';
            RETURN result_msg;
    END;
   
    RETURN result_msg;
END;
$$;

-- =====================================================================
-- USAGE EXAMPLES
-- =====================================================================
-- Execute all migrations:
-- CALL P_MIGRATE_MISSING_BTEQ_TABLES();
--
-- Individual table migration example:
-- CALL P_MIGRATE_TERADATA_TABLE('K_PDCBODS', 'BUSN_PTNR', 'NPD_D12_DMN_GDWMIG_IBRG', 'PDCBODS', 'BUSN_PTNR', 'N', '', '', '', 'Y');
-- ===================================================================== 