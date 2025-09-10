-- Test Script for P_DMVA_WRAPPER_PROCEDURE
-- This script demonstrates various usage scenarios

-- Set up the environment
use role r_dev_npd_d12_gdwmig;
use database npd_d12_dmn_gdwmig;
use schema migration_tracking_v2;
use warehouse wh_usr_npd_d12_gdwmig_001;

-- Example 1: Full Migration with Structure Setup (Default behavior)
-- This is equivalent to the original sampleCode.sql logic
CALL NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING_V2.P_DMVA_WRAPPER_PROCEDURE(
    'K_PDDSTG',                                    -- Source database/schema
    'DERV_ACCT_PATY_CHG',                         -- Source table
    'NPD_D12_DMN_GDWMIG',                         -- Target database
    'TMP',                                        -- Target schema
    'PDDSTG_DERV_ACCT_PATY_CHG_20250903'         -- Target table
    -- All other parameters use defaults:
    -- P_WITH_CHUNKING_YN = 'N'
    -- P_CHUNKING_COLUMN = null, P_CHUNKING_VALUE = null, P_CHUNKING_DATA_TYPE = null
    -- P_SKIP_WAITING_FOR_MIGRATION_TASKS = 'N'
    -- P_LOAD_TYPE = 'FULL'
    -- P_REFRESH_STRUCTURES_YN = 'Y'
);

-- Example 2: Incremental Load (No Data Cleanup)
-- Use this when you want to append/update data without clearing existing records
/*
CALL NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING_V2.P_DMVA_WRAPPER_PROCEDURE(
    'K_PDDSTG', 
    'DERV_ACCT_PATY_CHG', 
    'NPD_D12_DMN_GDWMIG', 
    'TMP', 
    'PDDSTG_DERV_ACCT_PATY_CHG_20250903',
    'N',                                          -- No chunking
    null, null, null,                             -- Chunking parameters (not used)
    'N',                                          -- Wait for migration tasks
    'INCREMENTAL',                                -- Incremental load (skips data cleanup)
    'Y'                                           -- Refresh structures
);
*/

-- Example 3: Data-Only Migration (Skip Structure Setup)
-- Use this when structures are already configured and you only need to migrate data
/*
CALL NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING_V2.P_DMVA_WRAPPER_PROCEDURE(
    'K_PDDSTG', 
    'DERV_ACCT_PATY_CHG', 
    'NPD_D12_DMN_GDWMIG', 
    'TMP', 
    'PDDSTG_DERV_ACCT_PATY_CHG_20250903',
    'N',                                          -- No chunking
    null, null, null,                             -- Chunking parameters (not used)
    'N',                                          -- Wait for migration tasks
    'FULL',                                       -- Full load (clears existing data)
    'N'                                           -- Skip structure refresh (Step 5 only)
);
*/

-- Example 4: Large Table Migration with Chunking
-- Use this for very large tables that need to be migrated in chunks
/*
CALL NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING_V2.P_DMVA_WRAPPER_PROCEDURE(
    'K_PDDSTG', 
    'LARGE_TRANSACTION_TABLE', 
    'NPD_D12_DMN_GDWMIG', 
    'TMP', 
    'MIGRATED_LARGE_TRANSACTION_TABLE',
    'Y',                                          -- Enable chunking
    'TRANSACTION_DATE',                           -- Chunking column
    '2024-01-01',                                 -- Chunking value (start date)
    'DATE',                                       -- Chunking data type
    'N',                                          -- Wait for migration tasks
    'FULL',                                       -- Full load
    'Y'                                           -- Refresh structures
);
*/

-- Example 5: Quick Re-run (Structures Exist, Incremental Data)
-- Use this for regular incremental updates when structures are already set up
/*
CALL NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING_V2.P_DMVA_WRAPPER_PROCEDURE(
    'K_PDDSTG', 
    'DERV_ACCT_PATY_CHG', 
    'NPD_D12_DMN_GDWMIG', 
    'TMP', 
    'PDDSTG_DERV_ACCT_PATY_CHG_20250903',
    'N',                                          -- No chunking
    null, null, null,                             -- Chunking parameters (not used)
    'Y',                                          -- Skip waiting (fire and forget)
    'INCREMENTAL',                                -- Incremental load
    'N'                                           -- Skip structure refresh
);
*/

-- Verification Queries
-- Use these to check the results after running the procedure

-- Check mapping rules
SELECT * FROM migration_tracking_v2.dmva_mapping_rules 
WHERE source_object_name = 'DERV_ACCT_PATY_CHG' 
ORDER BY created_ts DESC;

-- Check object metadata
SELECT * FROM migration_tracking_v2.dmva_object_info 
WHERE object_name = 'DERV_ACCT_PATY_CHG' 
AND schema_name = 'K_PDDSTG';

-- Check source-to-target mappings
SELECT c.database_name, c.schema_name, c.object_name, c.object_type, a.*
FROM migration_tracking_v2.dmva_source_to_target_mapping a
INNER JOIN migration_tracking_v2.dmva_object_info b ON
    b.object_name = 'DERV_ACCT_PATY_CHG'
    AND b.schema_name = 'K_PDDSTG'
    AND b.object_id = a.source_object_id
INNER JOIN migration_tracking_v2.dmva_object_info c ON
    c.object_id = a.target_object_id;

-- Check migration tasks
SELECT * FROM migration_tracking_v2.dmva_tasks 
WHERE source_object_id IN (
    SELECT object_id FROM migration_tracking_v2.dmva_object_info 
    WHERE object_name = 'DERV_ACCT_PATY_CHG' 
    AND schema_name = 'K_PDDSTG'
)
ORDER BY queue_ts DESC;

-- Check final data count
SELECT COUNT(1) as row_count 
FROM NPD_D12_DMN_GDWMIG.TMP.PDDSTG_DERV_ACCT_PATY_CHG_20250903; 