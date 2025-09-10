# Teradata to Snowflake Migration Procedure

## Overview

The `P_MIGRATE_TERADATA_TO_SNOWFLAKE` is a comprehensive stored procedure that automates the DMVA (Data Migration and Validation Automation) process for migrating tables from Teradata to Snowflake. This procedure implements the logic from `sampleCode.sql` with enhanced parameterization and conditional execution flows.

## Procedure Signature

```sql
CREATE OR REPLACE PROCEDURE NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING_V2.P_MIGRATE_TERADATA_TO_SNOWFLAKE(
    "P_SOURCE_DATABASE_NAME" VARCHAR,      -- Source Teradata database/schema name
    "P_SOURCE_TABLE_NAME" VARCHAR,         -- Source table name
    "P_TARGET_DATABASE_NAME" VARCHAR,      -- Target Snowflake database name
    "P_TARGET_SCHEMA_NAME" VARCHAR,        -- Target Snowflake schema name
    "P_TARGET_TABLE_NAME" VARCHAR,         -- Target table name
    "P_WITH_CHUNKING_YN" VARCHAR DEFAULT 'N',                    -- Enable chunking (Y/N)
    "P_CHUNKING_COLUMN" VARCHAR DEFAULT null,                    -- Column for chunking
    "P_CHUNKING_VALUE" VARCHAR DEFAULT null,                     -- Chunking value
    "P_CHUNKING_DATA_TYPE" VARCHAR DEFAULT null,                 -- Data type for chunking
    "P_SKIP_WAITING_FOR_MIGRATION_TASKS" VARCHAR DEFAULT 'N',    -- Skip task monitoring (Y/N)
    "P_LOAD_TYPE" VARCHAR DEFAULT 'FULL',                        -- Load type: FULL or INCREMENTAL
    "P_REFRESH_STRUCTURES_YN" VARCHAR DEFAULT 'Y'                -- Refresh structures (Y/N)
)
RETURNS VARCHAR
```

## New Parameters

### P_LOAD_TYPE (Default: 'FULL')
- **'FULL'**: Performs complete data refresh, including cleanup of existing data
- **'INCREMENTAL'**: Skips Step 4 (data cleanup), preserving existing data

### P_REFRESH_STRUCTURES_YN (Default: 'Y')
- **'Y'**: Executes all steps (1-6) including structure setup and data migration
- **'N'**: Skips to Step 5 only (data migration), assumes structures are already configured

## Execution Flow

### Normal Flow (P_REFRESH_STRUCTURES_YN = 'Y')

1. **Step 1: Configure Mapping Rules**
   - Inserts mapping rules into `dmva_mapping_rules` table
   - Checks for existing mappings to avoid duplicates

2. **Step 2: Get Metadata**
   - Calls `DMVA_GET_METADATA_TASKS` to populate object and column metadata
   - Monitors metadata tasks until completion
   - Validates metadata was successfully created

3. **Step 3: Create Sync Tables**
   - Calls `dmva_create_sync_tables` to create target structures and mappings
   - Verifies sync table creation

4. **Step 4: Fresh Migration Cleanup (Conditional)**
   - **If P_LOAD_TYPE = 'FULL'**: 
     - Deletes existing checksums
     - Clears target table data
   - **If P_LOAD_TYPE = 'INCREMENTAL'**: Skips cleanup

5. **Step 4.5: Configure Chunking (Optional)**
   - Configures chunking if `P_WITH_CHUNKING_YN = 'Y'`
   - Validates chunking parameters

6. **Step 5: Data Migration**
   - Calls `DMVA_GET_CHECKSUM_TASKS` to migrate data

7. **Step 6: Monitor Migration Tasks**
   - Monitors migration progress (unless skipped)
   - Provides final verification count

### Data-Only Flow (P_REFRESH_STRUCTURES_YN = 'N')

When `P_REFRESH_STRUCTURES_YN = 'N'`, the procedure jumps directly to **Step 5** (Data Migration), assuming all structures and mappings are already configured.

## Usage Examples

### Example 1: Full Migration with Structure Setup
```sql
CALL NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING_V2.P_MIGRATE_TERADATA_TO_SNOWFLAKE(
    'K_PDDSTG',                    -- Source database/schema
    'DERV_ACCT_PATY_CHG',         -- Source table
    'NPD_D12_DMN_GDWMIG',         -- Target database
    'TMP',                        -- Target schema
    'PDDSTG_DERV_ACCT_PATY_CHG_20250903',  -- Target table
    'N',                          -- No chunking
    null, null, null,             -- Chunking parameters (not used)
    'N',                          -- Wait for migration tasks
    'FULL',                       -- Full load
    'Y'                           -- Refresh structures
);
```

### Example 2: Incremental Load (No Data Cleanup)
```sql
CALL NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING_V2.P_MIGRATE_TERADATA_TO_SNOWFLAKE(
    'K_PDDSTG', 'DERV_ACCT_PATY_CHG', 'NPD_D12_DMN_GDWMIG', 'TMP', 
    'PDDSTG_DERV_ACCT_PATY_CHG_20250903',
    'N', null, null, null, 'N',
    'INCREMENTAL',                -- Incremental load (skips cleanup)
    'Y'
);
```

### Example 3: Data-Only Migration (Structures Already Exist)
```sql
CALL NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING_V2.P_MIGRATE_TERADATA_TO_SNOWFLAKE(
    'K_PDDSTG', 'DERV_ACCT_PATY_CHG', 'NPD_D12_DMN_GDWMIG', 'TMP', 
    'PDDSTG_DERV_ACCT_PATY_CHG_20250903',
    'N', null, null, null, 'N',
    'FULL',
    'N'                           -- Skip structure refresh (Step 5 only)
);
```

### Example 4: With Chunking
```sql
CALL NPD_D12_DMN_GDWMIG.MIGRATION_TRACKING_V2.P_MIGRATE_TERADATA_TO_SNOWFLAKE(
    'K_PDDSTG', 'LARGE_TABLE', 'NPD_D12_DMN_GDWMIG', 'TMP', 
    'MIGRATED_LARGE_TABLE',
    'Y',                          -- Enable chunking
    'DATE_COLUMN',                -- Chunking column
    '2024-01-01',                 -- Chunking value
    'DATE',                       -- Chunking data type
    'N', 'FULL', 'Y'
);
```

## Return Value

The procedure returns a detailed log string containing:
- Timestamp for each step
- Parameter values and validation results
- Progress updates for metadata and migration tasks
- Error messages (if any)
- Final verification counts

## Error Handling

The procedure includes comprehensive error handling:
- Parameter validation with descriptive error messages
- Metadata validation to ensure prerequisites are met
- Exception handling with detailed error reporting
- Graceful handling of missing or invalid configurations

## Dependencies

- `DMVA_GET_METADATA_TASKS` procedure
- `dmva_create_sync_tables` procedure
- `DMVA_GET_CHECKSUM_TASKS` procedure
- `P_SET_CHUNKING_KEY` procedure (for chunking functionality)
- DMVA metadata tables: `dmva_mapping_rules`, `dmva_object_info`, `dmva_tasks`, etc.

## Monitoring

The procedure provides real-time monitoring of:
- Metadata task completion
- Migration task progress
- Row counts before and after migration
- Task execution times and loop counters 