# STARCADPRODDATA Schema Recovery

## Overview
This folder contains scripts specifically for recovering and migrating the **STARCADPRODDATA** schema tables from Teradata to Snowflake Iceberg format.

## Files Description

### 1. `STARCADPRODDATA_Iceberg_Tables.sql`
- **Purpose**: Creates all iceberg table definitions for STARCADPRODDATA schema
- **Source**: Extracted from `CBA01-Creating Iceberg Tables.sql`
- **Content**: 
  - Schema setup commands
  - 80+ iceberg table CREATE statements
  - All table definitions with proper data types and comments

### 2. `STARCADPRODDATA_Migration_Execution.sql`
- **Purpose**: Snowflake procedure to execute all STARCADPRODDATA migrations
- **Source**: Extracted from `P_GDW1_POC_EXECUTE_ALL_MIGRATIONS.sql`
- **Content**:
  - Procedure `P_STARCADPRODDATA_EXECUTE_MIGRATIONS()`
  - 80+ migration calls using `P_MIGRATE_TERADATA_TABLE`
  - Progress tracking and error handling

## Execution Instructions

### Step 1: Create Iceberg Tables
```sql
-- Execute in Snowflake
@STARCADPRODDATA_Iceberg_Tables.sql
```

### Step 2: Run Migration Process
```sql
-- Execute the migration procedure
CALL P_STARCADPRODDATA_EXECUTE_MIGRATIONS();
```

## Table Categories Included

| Category | Count | Examples |
|----------|-------|----------|
| **APPT** (Application) | 15 | APPT, APPT_DEPT, APPT_PDCT, APPT_FEAT |
| **MAP_CSE** (Mapping) | 42 | MAP_CSE_APPT_C, MAP_CSE_PACK_PDCT_PL |
| **Account Tables** | 8 | ACCT_REL, ACCT_UNID_PATY, ACCT_XREF_* |
| **Event Tables** | 4 | EVNT, BUSN_EVNT, EVNT_EMPL, EVNT_INT_GRUP |
| **Interest Groups** | 5 | INT_GRUP, INT_GRUP_DEPT, INT_GRUP_EMPL |
| **Utility Tables** | 4 | UTIL_BTCH_ISAC, UTIL_ETI_CONV, UTIL_PARM, UTIL_PROS_ISAC |
| **Other** | 8 | THA_ACCT, MOS_LOAN, MOS_FCLY, PATY_* |

## Key Features

### Partitioning Strategy
- **Date Partitioned**: BUSN_EVNT, EVNT, EVNT_EMPL, DERV_ACCT_PATY
- **Non-Partitioned**: Most mapping and reference tables
- **Partition Column**: Usually `EFFT_D` (Effective Date)

### Data Volume Considerations
- **BUSN_EVNT**: Large historical table with yearly partitions (2014-2026)
- **MAP Tables**: Generally small reference data
- **APPT Tables**: Medium volume transactional data

### Migration Parameters
```sql
P_MIGRATE_TERADATA_TABLE(
    source_schema,      -- 'K_STAR_CAD_PROD_DATA'
    source_table,       -- Source table name
    target_database,    -- 'NPD_D12_DMN_GDWMIG_IBRG'
    target_schema,      -- 'STARCADPRODDATA'
    target_table,       -- Target table name
    is_partitioned,     -- 'Y' or 'N'
    partition_column,   -- Usually 'EFFT_D' or ''
    partition_interval, -- 'day', 'month', or ''
    partition_strategy, -- 'by_date' or ''
    enable_migration    -- 'Y'
)
```

## Notes

1. **Prerequisites**: Ensure `P_MIGRATE_TERADATA_TABLE` procedure exists
2. **Dependencies**: Some tables may have foreign key dependencies
3. **Performance**: Large tables (BUSN_EVNT) may take significant time
4. **Monitoring**: Check procedure return messages for progress updates
5. **Rollback**: Keep track of completed migrations for potential rollback

## Troubleshooting

### Common Issues
- **Permissions**: Ensure role has CREATE TABLE privileges on target database
- **Schema Existence**: STARCADPRODDATA schema must exist before migration
- **Data Types**: Some Teradata types may need manual adjustment
- **Constraints**: Foreign key constraints may need to be recreated after migration

### Verification Queries
```sql
-- Check created tables
SELECT TABLE_NAME, ROW_COUNT 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'STARCADPRODDATA'
ORDER BY TABLE_NAME;

-- Check migration status
SELECT * FROM migration_tracking.migration_log 
WHERE target_schema = 'STARCADPRODDATA'
ORDER BY migration_timestamp DESC;
```

## Contact
For questions about this migration process, refer to the main migration documentation or contact the data migration team. 