# BTEQ to DBT Conversion Summary

## Conversion Statistics

**Generated**: 2025-08-21 16:26:38  
**Source Directory**: ../Original Files  
**Target Directory**: OutputTest13  
**Project Name**: BTEQ_TEST13

## Files Processed

| File | Category | Size (KB) | Lines | Complexity | Variables |
|------|----------|-----------|-------|------------|-----------|
| ACCT_BALN_BKDT_ADJ_RULE_ISRT.sql | account_balance | 4.9 | 164 | 38 | VTECH |
| ACCT_BALN_BKDT_AUDT_GET_PROS_KEY.sql | account_balance | 5.4 | 177 | 34 |  |
| ACCT_BALN_BKDT_AUDT_ISRT.sql | account_balance | 3.7 | 114 | 42 | VTECH, DDSTG |
| ACCT_BALN_BKDT_AVG_CALL_PROC.sql | account_balance | 1.1 | 35 | 20 | CAD_PROD_MACRO |
| ACCT_BALN_BKDT_DELT.sql | account_balance | 1.8 | 50 | 23 |  |
| ACCT_BALN_BKDT_GET_PROS_KEY.sql | account_balance | 5.3 | 178 | 35 |  |
| ACCT_BALN_BKDT_ISRT.sql | account_balance | 1.9 | 67 | 23 | DDSTG |
| ACCT_BALN_BKDT_RECN_GET_PROS_KEY.sql | account_balance | 5.2 | 175 | 34 |  |
| ACCT_BALN_BKDT_RECN_ISRT.sql | account_balance | 5.0 | 238 | 80 | VTECH, DDSTG |
| ACCT_BALN_BKDT_STG_ISRT.sql | account_balance | 5.1 | 174 | 61 | DDSTG, VTECH |
| ACCT_BALN_BKDT_UTIL_PROS_UPDT.sql | account_balance | 1.3 | 48 | 27 |  |
| GDW1-BTEQ.snowct | configuration | 4.9 | 151 | 0 |  |
| source_table_mapping.txt | configuration | 3.1 | 89 | 0 |  |
| storage | configuration | 56.0 | 398 | 24 |  |
| BTEQ_SAP_EDO_WKLY_LOAD.sql | data_loading | 20.9 | 638 | 255 | UTILSTG, STARDATADB, VTECH (+3 more) |
| BTEQ_TAX_INSS_MNLY_LOAD.sql | data_loading | 8.2 | 320 | 123 | UTILSTG, STARDATADB, VTECH (+1 more) |
| bteq_login.sql | data_loading | 0.0 | 1 | 3 |  |
| DERV_ACCT_PATY_00_DATAWATCHER.sql | derived_account_party | 5.3 | 173 | 67 | SRCE_SYST_M, VPATY, VTECH |
| DERV_ACCT_PATY_01_SP_GET_BTCH_KEY.sql | derived_account_party | 1.7 | 63 | 41 | STARMACRDB, SRCE_SYST_M |
| DERV_ACCT_PATY_01_SP_GET_PROS_KEY.sql | derived_account_party | 2.4 | 88 | 59 | RSTR_F, SRCE_M, STARMACRDB (+4 more) |
| DERV_ACCT_PATY_02_CRAT_WORK_TABL.sql | derived_account_party | 45.7 | 1028 | 949 | UCB, VTECH, DDSTG |
| DERV_ACCT_PATY_02_CRAT_WORK_TABL_CHG0379808.sql | derived_account_party | 32.7 | 800 | 753 | UCB, VTECH, DDSTG |
| DERV_ACCT_PATY_03_SET_ACCT_PRTF.sql | derived_account_party | 3.0 | 87 | 61 | DDSTG, VTECH |
| DERV_ACCT_PATY_04_POP_CURR_TABL.sql | derived_account_party | 42.3 | 1254 | 433 | VTECH, DDSTG |
| DERV_ACCT_PATY_05_SET_PRTF_PRFR_FLAG.sql | derived_account_party | 10.2 | 301 | 129 | DDSTG |
| DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG.sql | derived_account_party | 12.7 | 333 | 128 | DDSTG |
| DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG_CHG0379808.sql | derived_account_party | 7.5 | 208 | 89 | DDSTG |
| DERV_ACCT_PATY_07_CRAT_DELTAS.sql | derived_account_party | 4.6 | 152 | 90 | VTECH, DDSTG |
| DERV_ACCT_PATY_08_APPLY_DELTAS.sql | derived_account_party | 7.5 | 228 | 116 | STARDATADB, TBSHORT, VTECH (+1 more) |
| DERV_ACCT_PATY_99_DROP_WORK_TABL.sql | derived_account_party | 5.3 | 226 | 251 | ENV_C, DDSTG |
| DERV_ACCT_PATY_99_DROP_WORK_TABL_CHG0379808.sql | derived_account_party | 4.9 | 208 | 230 | ENV_C, DDSTG |
| DERV_ACCT_PATY_99_SP_COMT_BTCH_KEY.sql | derived_account_party | 1.2 | 47 | 28 |  |
| DERV_ACCT_PATY_99_SP_COMT_PROS_KEY.sql | derived_account_party | 1.6 | 58 | 30 |  |
| source_tables.txt | misc | 1.1 | 64 | 1 | DDSTG |
| target_tables.txt | misc | 0.7 | 35 | 0 |  |
| prtf_tech_acct_int_grup_psst.sql | portfolio_technical | 7.8 | 286 | 98 | STARDATADB, VTECH |
| prtf_tech_acct_own_rel_psst.sql | portfolio_technical | 3.0 | 106 | 40 | STARDATADB, VTECH |
| prtf_tech_acct_psst.sql | portfolio_technical | 7.4 | 269 | 74 | STARDATADB, VTECH |
| prtf_tech_acct_rel_psst.sql | portfolio_technical | 3.8 | 123 | 42 | STARDATADB, VTECH |
| prtf_tech_daly_datawatcher_c.sql | portfolio_technical | 2.6 | 94 | 34 | VTECH |
| prtf_tech_grd_prtf_type_enhc_psst.sql | portfolio_technical | 1.9 | 80 | 58 | DGRDDB, VTECH |
| prtf_tech_int_grup_enhc_psst.sql | portfolio_technical | 5.9 | 210 | 85 | STARDATADB, VTECH |
| prtf_tech_int_grup_own_psst.sql | portfolio_technical | 11.3 | 328 | 102 | STARDATADB, VTECH |
| prtf_tech_int_psst.sql | portfolio_technical | 2.2 | 78 | 39 | STARDATADB, VTECH |
| prtf_tech_own_psst.sql | portfolio_technical | 13.4 | 406 | 136 | STARDATADB, VTECH |
| prtf_tech_own_rel_psst.sql | portfolio_technical | 3.9 | 132 | 42 | STARDATADB, VTECH |
| prtf_tech_paty_int_grup_psst.sql | portfolio_technical | 7.3 | 252 | 99 | STARDATADB, VTECH |
| prtf_tech_paty_own_rel_psst.sql | portfolio_technical | 3.1 | 106 | 40 | STARDATADB, VTECH |
| prtf_tech_paty_psst.sql | portfolio_technical | 6.5 | 228 | 73 | STARDATADB, VTECH |
| prtf_tech_paty_rel_psst.sql | portfolio_technical | 3.7 | 123 | 42 | STARDATADB, VTECH |
| sp_comt_btch_key.sql | process_control | 1.2 | 47 | 28 |  |
| sp_comt_pros_key.sql | process_control | 1.3 | 52 | 31 |  |
| sp_get_btch_key.sql | process_control | 1.5 | 62 | 48 | INDATE, STARMACRDB, SRCE_SYST_M |
| sp_get_pros_key.sql | process_control | 1.5 | 59 | 44 | RSTR_F, SRCE_M, STARMACRDB (+4 more) |

## Conversion Patterns Applied

### 1. BTEQ Commands Removed
- `.RUN FILE`, `.IF ERRORCODE`, `.GOTO`, `.SET` commands
- Replaced with DBT configuration and hooks

### 2. Variable References Converted
- `%%VARIABLE%%` → `{{ bteq_var('VARIABLE') }}`

### 3. Volatile Tables → CTEs
- `CREATE MULTISET VOLATILE TABLE` → `WITH table_name AS (...)`

### 4. File Operations → Stage Operations  
- `.IMPORT/.EXPORT` → Snowflake stage operations (commented)

### 5. Teradata Functions → Snowflake Functions
- `ADD_MONTHS()` → `DATEADD(MONTH, ...)`
- `DATE'YYYY-MM-DD'` → `'YYYY-MM-DD'::DATE`

## Next Steps

1. **Review Generated Models**: Validate conversion accuracy
2. **Configure Variables**: Update `dbt_project.yml` with correct values
3. **Test Models**: Run `dbt run` and `dbt test`
4. **Customize Logic**: Adjust business logic as needed
5. **Deploy**: Move to production environment

## Support

- Review generated README.md for setup instructions
- Check individual model files for conversion notes
- Refer to DBT documentation for advanced configurations
