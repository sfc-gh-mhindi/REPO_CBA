# BTEQ Analysis Results - 20250820_123917

## Overview

This directory contains a comprehensive analysis of **49 BTEQ files** converted from Teradata to Snowflake SQL using the advanced BTEQ Parser Service with SQLGlot integration.

## Directory Structure

```
bteq_analysis_20250820_123917/
├── README.md                           # This file - summary overview
├── bteq_migration_analysis.md          # Comprehensive analysis report
├── analysis_summary.json              # Machine-readable summary data
└── individual/                        # Individual file analyses
    ├── [filename]_analysis.md          # Detailed analysis per file
    └── ...
```

## Analysis Summary


### 📊 Overall Statistics

| Metric | Value |
|--------|-------|
| **Total Files Analyzed** | 49 |
| **Successfully Processed** | 47 |
| **Failed to Process** | 2 |
| **Total Control Statements** | 744 |
| **Total SQL Blocks** | 173 |

## 📋 File Analysis Summary

| File Name | Status | Controls | SQL Blocks | Complexity | Features | Migration Strategy | SQL Types |
|-----------|--------|----------|------------|------------|----------|-------------------|-----------|
| ACCT_BALN_BKDT_ADJ_RULE_ISRT.sql | ✅ | 7 | 2 | 4 | 6 | Simple Model | DELETE, INSERT |
| ACCT_BALN_BKDT_AUDT_GET_PROS_KEY.sql | ✅ | 6 | 1 | 297 | 1 | Complex Model | INSERT |
| ACCT_BALN_BKDT_AUDT_ISRT.sql | ✅ | 7 | 2 | 195 | 1 | Incremental Model | INSERT, UPDATE |
| ACCT_BALN_BKDT_AVG_CALL_PROC.sql | ✅ | 7 | 0 | 0 | 0 | dbt Macro | Other |
| ACCT_BALN_BKDT_DELT.sql | ✅ | 6 | 1 | 102 | 1 | Incremental Model | DELETE |
| ACCT_BALN_BKDT_GET_PROS_KEY.sql | ✅ | 6 | 1 | 307 | 1 | Complex Model | INSERT |
| ACCT_BALN_BKDT_ISRT.sql | ✅ | 6 | 1 | 52 | 1 | Incremental Model | INSERT |
| ACCT_BALN_BKDT_RECN_GET_PROS_KEY.sql | ✅ | 6 | 1 | 297 | 1 | Complex Model | INSERT |
| ACCT_BALN_BKDT_RECN_ISRT.sql | ✅ | 11 | 5 | 57 | 1 | Incremental Model | DELETE, INSERT, UPDATE |
| ACCT_BALN_BKDT_STG_ISRT.sql | ✅ | 9 | 4 | 412 | 2 | Complex Model | DELETE, INSERT |
| ACCT_BALN_BKDT_UTIL_PROS_UPDT.sql | ✅ | 6 | 1 | 0 | 1 | Simple Model | UPDATE |
| BTEQ_SAP_EDO_WKLY_LOAD.sql | ✅ | 32 | 11 | 784 | 2 | Complex Model | DELETE, INSERT, SELECT |
| BTEQ_TAX_INSS_MNLY_LOAD.sql | ✅ | 19 | 7 | 232 | 3 | Complex Model | INSERT, SELECT, UPDATE |
| DERV_ACCT_PATY_00_DATAWATCHER.sql | ✅ | 12 | 2 | 257 | 2 | Complex Model | SELECT |
| DERV_ACCT_PATY_01_SP_GET_BTCH_KEY.sql | ✅ | 12 | 0 | 0 | 0 | dbt Macro | Other |
| DERV_ACCT_PATY_01_SP_GET_PROS_KEY.sql | ✅ | 16 | 1 | 24 | 1 | Simple Model | SELECT |
| DERV_ACCT_PATY_02_CRAT_WORK_TABL.sql | ❌ | 0 | 0 | - | - | Manual Review | Error |
| DERV_ACCT_PATY_02_CRAT_WORK_TABL_CHG0379808.sql | ❌ | 0 | 0 | - | - | Manual Review | Error |
| DERV_ACCT_PATY_03_SET_ACCT_PRTF.sql | ✅ | 12 | 4 | 60 | 1 | Incremental Model | DELETE, INSERT |
| DERV_ACCT_PATY_04_POP_CURR_TABL.sql | ✅ | 91 | 21 | 4052 | 3 | Complex Model | DELETE, INSERT |
| DERV_ACCT_PATY_05_SET_PRTF_PRFR_FLAG.sql | ✅ | 21 | 6 | 381 | 3 | Complex Model | DELETE, INSERT, UPDATE |
| DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG.sql | ✅ | 19 | 7 | 725 | 3 | Complex Model | DELETE, INSERT, UPDATE |
| DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG_CHG0379808.sql | ✅ | 13 | 5 | 305 | 3 | Complex Model | DELETE, INSERT, UPDATE |
| DERV_ACCT_PATY_07_CRAT_DELTAS.sql | ✅ | 17 | 6 | 334 | 1 | Complex Model | DELETE, INSERT |
| DERV_ACCT_PATY_08_APPLY_DELTAS.sql | ✅ | 20 | 8 | 340 | 3 | Complex Model | DELETE, INSERT, SELECT, UPDATE |
| DERV_ACCT_PATY_99_DROP_WORK_TABL.sql | ✅ | 70 | 0 | 0 | 0 | dbt Macro | Other |
| DERV_ACCT_PATY_99_DROP_WORK_TABL_CHG0379808.sql | ✅ | 64 | 0 | 0 | 0 | dbt Macro | Other |
| DERV_ACCT_PATY_99_SP_COMT_BTCH_KEY.sql | ✅ | 8 | 1 | 32 | 1 | Simple Model | UPDATE |
| DERV_ACCT_PATY_99_SP_COMT_PROS_KEY.sql | ✅ | 8 | 1 | 0 | 1 | Simple Model | UPDATE |
| bteq_login.sql | ✅ | 1 | 0 | 0 | 0 | dbt Macro | Other |
| prtf_tech_acct_int_grup_psst.sql | ✅ | 16 | 8 | 686 | 5 | Complex Model | DELETE, INSERT |
| prtf_tech_acct_own_rel_psst.sql | ✅ | 11 | 2 | 165 | 1 | Incremental Model | DELETE, INSERT |
| prtf_tech_acct_psst.sql | ✅ | 12 | 6 | 635 | 5 | Complex Model | DELETE, INSERT |
| prtf_tech_acct_rel_psst.sql | ✅ | 11 | 2 | 236 | 1 | Complex Model | DELETE, INSERT |
| prtf_tech_daly_datawatcher_c.sql | ✅ | 8 | 1 | 143 | 2 | Incremental Model | SELECT |
| prtf_tech_grd_prtf_type_enhc_psst.sql | ✅ | 12 | 4 | 88 | 1 | Incremental Model | DELETE, INSERT |
| prtf_tech_int_grup_enhc_psst.sql | ✅ | 15 | 6 | 605 | 5 | Complex Model | DELETE, INSERT |
| prtf_tech_int_grup_own_psst.sql | ✅ | 16 | 8 | 358 | 5 | Complex Model | DELETE, INSERT |
| prtf_tech_int_psst.sql | ✅ | 8 | 2 | 82 | 1 | Incremental Model | DELETE, INSERT |
| prtf_tech_own_psst.sql | ✅ | 21 | 12 | 1202 | 5 | Complex Model | DELETE, INSERT |
| prtf_tech_own_rel_psst.sql | ✅ | 11 | 2 | 276 | 1 | Complex Model | DELETE, INSERT |
| prtf_tech_paty_int_grup_psst.sql | ✅ | 16 | 8 | 681 | 5 | Complex Model | DELETE, INSERT |
| prtf_tech_paty_own_rel_psst.sql | ✅ | 11 | 2 | 165 | 1 | Incremental Model | DELETE, INSERT |
| prtf_tech_paty_psst.sql | ✅ | 12 | 6 | 559 | 5 | Complex Model | DELETE, INSERT |
| prtf_tech_paty_rel_psst.sql | ✅ | 11 | 2 | 236 | 1 | Complex Model | DELETE, INSERT |
| sp_comt_btch_key.sql | ✅ | 8 | 1 | 32 | 1 | Simple Model | UPDATE |
| sp_comt_pros_key.sql | ✅ | 8 | 1 | 54 | 1 | Incremental Model | UPDATE |
| sp_get_btch_key.sql | ✅ | 13 | 1 | 0 | 1 | Simple Model | SELECT |
| sp_get_pros_key.sql | ✅ | 12 | 0 | 0 | 0 | dbt Macro | Other |

## 🎛️ Control Statement Analysis

| Control Type | Count | Migration Approach |
|--------------|-------|--------------------|
| CALL_SP | 5 | DCF stored procedure macro |
| COLLECT_STATS | 54 | Snowflake post-hook |
| EXPORT | 22 | dbt post-hook or external process |
| IF_ERRORCODE | 397 | DCF error handling macro |
| IMPORT | 45 | dbt seeds or external tables |
| LABEL | 93 | DCF checkpoint/label |
| LOGOFF | 73 | Remove (handled by dbt profiles) |
| LOGON | 1 | Remove (handled by dbt profiles) |
| OS_CMD | 8 | dbt post-hook or external process |
| RUN | 46 | dbt pre-hook or external script |

## 🎯 Teradata Features Requiring Migration

| Feature | Occurrences | Conversion Status | Migration Notes |
|---------|-------------|-------------------|-----------------|
| Variable substitution | 167 | ✅ Automatic | Replace with dbt variables |
| QUALIFY clause | 28 | ⚠️ Manual | Convert to window function + WHERE |
| ROW_NUMBER() OVER | 22 | ✅ Automatic | Direct Snowflake support |
| ADD_MONTHS function | 17 | ✅ Automatic | Use DATEADD in Snowflake |
| EXTRACT with complex syntax | 9 | ⚠️ Manual | Simplify extraction logic |
| YEAR(4) TO MONTH intervals | 1 | ❌ Manual | Complex date arithmetic conversion |

## ⚠️ Files Requiring Manual Attention

The following files failed automated processing and require manual review:

| File Name | Error | Recommendation |
|-----------|-------|----------------|
| DERV_ACCT_PATY_02_CRAT_WORK_TABL_CHG0379808.sql | 'utf-8' codec can't decode byte 0x96 in position 22201: invalid start byte | Check file encoding - may need conversion from EBCDIC/Latin-1 |
| DERV_ACCT_PATY_02_CRAT_WORK_TABL.sql | 'utf-8' codec can't decode byte 0x96 in position 22523: invalid start byte | Check file encoding - may need conversion from EBCDIC/Latin-1 |


## 🚀 Next Steps

1. **Review Individual Analysis**: Check `individual/` directory for detailed file-by-file analysis
2. **Prioritize Migration**: Start with Simple Models, then Incremental, then Complex
3. **DCF Integration**: Use control statement mappings for DCF macro implementation  
4. **Teradata Features**: Address high-occurrence features first for maximum impact
5. **Manual Review**: Handle failed files and complex manual conversion cases

## 📝 Notes

- **Complexity Score**: Based on SQL AST node count (higher = more complex)
- **Migration Strategy**: Recommended dbt approach based on complexity and features
- **SQL Types**: Primary SQL operation types found in each file
- **Features**: Count of unique Teradata-specific features requiring conversion

---

*Generated by BTEQ Parser Service - 2025-08-20 12:39:25*
