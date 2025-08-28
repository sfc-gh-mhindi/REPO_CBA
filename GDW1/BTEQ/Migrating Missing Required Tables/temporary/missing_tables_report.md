# Missing Tables Analysis Report

## Summary

This analysis compares the 109 unique tables extracted from BTEQ files (via SQLGlot parsing) against tables and views available in the Snowflake migration SQL files.

### Coverage Statistics
- **Total tables in BTEQ JSON**: 109
- **Available in SQL files**: 103 (94.5%)
- **Missing from SQL files**: 6 (5.5%)

### SQL Files Analyzed
1. **CBA01.1 - Missing Tables.sql**: 46 Iceberg tables
2. **CBA01-Creating Iceberg Tables.sql**: 138 Iceberg tables  
3. **CBA02-Creating Views.sql**: 133 views
4. **Total available**: 314 tables/views (with some overlap)

## Missing Tables Details

### ❌ **6 Tables Missing from All SQL Files**

#### 1. **PVTECH Schema (1 table)**
- `GRD_PRTF_TYPE_ENHC`

#### 2. **STAR_CAD_PROD_DATA Schema (1 table)**
- `ACCT_PATY_TAX_INSS`

#### 3. **Unqualified Tables (4 tables)**
- `ACCT_PATY_INSS_FIX`
- `ACCT_REL_TRNF_PRTP_2`
- `LOAD_MISSING_ACCTS1`
- `UTIL_PROS_SAP_RUN`

## Analysis Notes

### Schema Mapping Success
The analysis successfully mapped tables across different schema naming conventions:
- `STAR_CAD_PROD_DATA` ↔ `STARCADPRODDATA`
- `PDDSTG` ↔ `PVDSTG`
- Most BTEQ table references were successfully found in either Iceberg tables or views

### Coverage by Schema (Original BTEQ References)
- **PDDSTG**: ✅ All 28 tables covered
- **PVTECH**: ✅ 46/47 tables covered (97.9%)
- **STAR_CAD_PROD_DATA**: ✅ 24/25 tables covered (96.0%)
- **PVDATA**: ✅ All 2 tables covered
- **PVPATY**: ✅ All 1 table covered
- **DGRDDB**: ✅ Both tables appear to be covered by equivalent tables in other schemas
- **Unqualified**: ❌ 0/4 tables covered (0%)

## Recommendations

### 1. **High Priority - Unqualified Tables**
The 4 unqualified tables need investigation:
- These may be temporary tables, staging tables, or tables from external systems
- **Action**: Review BTEQ scripts to understand their purpose and determine if they need to be created

### 2. **Medium Priority - Schema-Qualified Missing Tables**
Two tables from known schemas are missing:
- `PVTECH.GRD_PRTF_TYPE_ENHC`: May need to be added to the PVTECH views
- `STAR_CAD_PROD_DATA.ACCT_PATY_TAX_INSS`: May need to be added to STARCADPRODDATA Iceberg tables

### 3. **Data Lineage Verification**
- Verify that the 103 found tables/views provide equivalent functionality to the original BTEQ table references
- Some tables may have been renamed or consolidated during the migration

## Conclusion

With 94.5% coverage, the migration SQL files provide excellent coverage of the tables referenced in the BTEQ scripts. The 6 missing tables represent a small gap that should be investigated to ensure complete migration coverage.

The analysis demonstrates that the schema mapping and table creation strategies have been highly effective in providing Snowflake equivalents for the vast majority of Teradata table references found in the BTEQ code. 