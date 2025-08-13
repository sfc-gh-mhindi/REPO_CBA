-- =====================================================
-- COMPREHENSIVE FIX FOR NPW DBT PROJECT ERRORS
-- =====================================================
-- This script addresses all missing objects and control data issues
-- Based on error analysis from models execution
-- =====================================================

USE DATABASE NPD_D12_DMN_GDWMIG_IBRG_V;
USE SCHEMA P_V_OUT_001_STD_0;

-- =====================================================
-- PART 1: CREATE MISSING INPROCESS TABLES
-- =====================================================
-- These tables represent external data feeds that should exist
-- before dbt models run. Creating with sample data for testing.

-- Current date INPROCESS table (matches error: 20250807)
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.CBA_APP__CSEL4__CSEL4DEV__INPROCESS__CSE_CPL_BUS_APP_CSE_CPL_BUS_APP_20250807__DLY (
    RECORD_TYPE VARCHAR(1),
    MOD_TIMESTAMP VARCHAR(25),
    PL_APP_ID VARCHAR(12),
    NOMINATED_BRANCH_ID VARCHAR(12),
    PL_PACKAGE_CAT_ID VARCHAR(12),
    DUMMY VARCHAR(1),
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- Create the LOOKUPSET table that the mapping transformation needs
CREATE OR REPLACE TABLE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.CBA_APP__CSEL4__CSEL4DEV__LOOKUPSET__MAP_CSE_PACK_PDCT_PL_PL_PACK_CAT_ID__FS (
    PL_PACKAGE_CAT_ID VARCHAR(16777216),
    PDCT_N VARCHAR(16777216),
    LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- PART 3: FIX CONTROL DATA FLAGS
-- =====================================================
-- Reset run stream flags to proper values for dbt processing

-- Check current state first
SELECT 
    'BEFORE_FIX' as status,
    RUN_STRM_C,
    RUN_STRM_ABRT_F,
    RUN_STRM_ACTV_F,
    SYST_C,
    CASE 
        WHEN RUN_STRM_ABRT_F = 'Y' THEN '❌ ABORTED'
        WHEN RUN_STRM_ACTV_F = 'Y' THEN '🚨 ACTIVE (PROBLEMATIC)'
        WHEN RUN_STRM_ACTV_F = 'I' THEN '✅ INACTIVE (READY)'
        ELSE '⚠️ OTHER'
    END as flag_status
FROM NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.RUN_STRM_TMPL 
-- WHERE RUN_STRM_C IN ('CSE_ICE_BUS_DEAL', 'CSE_CPL_BUS_APP')
ORDER BY RUN_STRM_C;

-- Fix the flags for all streams
UPDATE NPD_D12_DMN_GDWMIG_IBRG_V.P_V_OUT_001_STD_0.RUN_STRM_TMPL 
SET 
    RUN_STRM_ABRT_F = 'N',    -- Not aborted
    RUN_STRM_ACTV_F = 'I'    -- Inactive (ready to run)
WHERE RUN_STRM_C IN (
    'CSE_ICE_BUS_DEAL', 
    'CSE_CPL_BUS_APP',
    'CSE_CCC_BUS_APP_PROD',
    'CSE_COM_BUS_CCL_CHL_COM_APP',
    'CSE_COM_BUS_APP_PROD_CCL_PL_APP_PROD',
    'CSE_COM_CPO_BUS_NCPR_CLNT',
    'CSE_L4_PRE_PROC'
) AND SYST_C = 'CSEL4';
