# BTEQ_TEST01 - BTEQ to DBT Migration

## Overview

This DBT project was automatically generated from 51 BTEQ files using the BTEQ to DBT converter.
The project follows DBT best practices and includes DCF framework integration for enterprise-grade logging and process control.

## Project Statistics

- **Total Files Converted**: 51
- **Categories**: 6
- **Total Lines of Code**: 11,223
- **Average Complexity Score**: 106.5

## File Categories

- **Derived Account Party**: 16 files, 5,254 lines, complexity 215.9
- **Account Balance**: 11 files, 1,420 lines, complexity 37.9
- **Process Control**: 4 files, 220 lines, complexity 37.8
- **Portfolio Technical**: 15 files, 2,821 lines, complexity 66.9
- **Configuration**: 2 files, 549 lines, complexity 12.0
- **Data Loading**: 3 files, 959 lines, complexity 127.0

## Setup Instructions

### 1. Environment Setup
```bash
# Install DBT
pip install dbt-snowflake

# Navigate to project
cd /Users/mhindi/Library/CloudStorage/GoogleDrive-mazen.hindi@snowflake.com/My Drive/Work/Code Repos/CBA/REPO_CBA/GDW1/BTEQ/BTEQ to DBT Conversion Tool/OutputTest3

# Install dependencies
dbt deps
```

### 2. Configuration
Set environment variables:
```bash
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_ROLE=your_role
export SNOWFLAKE_WAREHOUSE=your_warehouse
```

### 3. Variable Configuration
Update variables in `dbt_project.yml`:
```yaml
vars:
  BTEQ_LOGON_SCRIPT: "YOUR_BTEQ_LOGON_SCRIPT_VALUE"
  CAD_PROD_DATA: "YOUR_CAD_PROD_DATA_VALUE"
  CAD_PROD_MACRO: "YOUR_CAD_PROD_MACRO_VALUE"
  DDSTG: "YOUR_DDSTG_VALUE"
  DGRDDB: "YOUR_DGRDDB_VALUE"
  ENV_C: "YOUR_ENV_C_VALUE"
  GDW_HOST: "YOUR_GDW_HOST_VALUE"
  GDW_PASS: "YOUR_GDW_PASS_VALUE"
  GDW_USER: "YOUR_GDW_USER_VALUE"
  INDATE: "YOUR_INDATE_VALUE"
  PSST_TABLE_M: "YOUR_PSST_TABLE_M_VALUE"
  RSTR_F: "YOUR_RSTR_F_VALUE"
  SRCE_M: "YOUR_SRCE_M_VALUE"
  SRCE_SYST_M: "YOUR_SRCE_SYST_M_VALUE"
  STARDATADB: "YOUR_STARDATADB_VALUE"
  STARMACRDB: "YOUR_STARMACRDB_VALUE"
  STRM_C: "YOUR_STRM_C_VALUE"
  TBSHORT: "YOUR_TBSHORT_VALUE"
  UCB: "YOUR_UCB_VALUE"
  UTILSTG: "YOUR_UTILSTG_VALUE"
  VCBODS: "YOUR_VCBODS_VALUE"
  VEXTR: "YOUR_VEXTR_VALUE"
  VPATY: "YOUR_VPATY_VALUE"
  VTECH: "YOUR_VTECH_VALUE"
```

### 4. Initial Testing
```bash
# Test connection
dbt debug

# Compile models
dbt compile

# Run models
dbt run

# Run tests
dbt test
```

## Model Structure

```
models/
├── staging/
├── intermediate/
│   ├── Account Balance
│   ├── Derived Account Party
│   ├── Portfolio Technical
│   ├── Process Control
└── marts/
    └── Account Balance
    └── Derived Account Party
    └── Portfolio Technical
    └── Process Control
```


## Generated Files


### Derived Account Party
- `int_derv_acct_paty_00_datawatcher.sql` ← `DERV_ACCT_PATY_00_DATAWATCHER.sql` (5.3KB, 173 lines)
- `int_derv_acct_paty_01_sp_get_btch_key.sql` ← `DERV_ACCT_PATY_01_SP_GET_BTCH_KEY.sql` (1.7KB, 63 lines)
- `int_derv_acct_paty_01_sp_get_pros_key.sql` ← `DERV_ACCT_PATY_01_SP_GET_PROS_KEY.sql` (2.4KB, 88 lines)
- `int_derv_acct_paty_02_crat_work_tabl.sql` ← `DERV_ACCT_PATY_02_CRAT_WORK_TABL.sql` (45.7KB, 1028 lines)
- `int_derv_acct_paty_02_crat_work_tabl_chg0379808.sql` ← `DERV_ACCT_PATY_02_CRAT_WORK_TABL_CHG0379808.sql` (32.7KB, 800 lines)
- `int_derv_acct_paty_03_set_acct_prtf.sql` ← `DERV_ACCT_PATY_03_SET_ACCT_PRTF.sql` (3.0KB, 87 lines)
- `int_derv_acct_paty_04_pop_curr_tabl.sql` ← `DERV_ACCT_PATY_04_POP_CURR_TABL.sql` (42.3KB, 1254 lines)
- `int_derv_acct_paty_05_set_prtf_prfr_flag.sql` ← `DERV_ACCT_PATY_05_SET_PRTF_PRFR_FLAG.sql` (10.2KB, 301 lines)
- `int_derv_acct_paty_06_set_max_prfr_flag.sql` ← `DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG.sql` (12.7KB, 333 lines)
- `int_derv_acct_paty_06_set_max_prfr_flag_chg0379808.sql` ← `DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG_CHG0379808.sql` (7.5KB, 208 lines)
- `int_derv_acct_paty_07_crat_deltas.sql` ← `DERV_ACCT_PATY_07_CRAT_DELTAS.sql` (4.6KB, 152 lines)
- `int_derv_acct_paty_08_apply_deltas.sql` ← `DERV_ACCT_PATY_08_APPLY_DELTAS.sql` (7.5KB, 228 lines)
- `int_derv_acct_paty_99_drop_work_tabl.sql` ← `DERV_ACCT_PATY_99_DROP_WORK_TABL.sql` (5.3KB, 226 lines)
- `int_derv_acct_paty_99_drop_work_tabl_chg0379808.sql` ← `DERV_ACCT_PATY_99_DROP_WORK_TABL_CHG0379808.sql` (4.9KB, 208 lines)
- `int_derv_acct_paty_99_sp_comt_btch_key.sql` ← `DERV_ACCT_PATY_99_SP_COMT_BTCH_KEY.sql` (1.2KB, 47 lines)
- `int_derv_acct_paty_99_sp_comt_pros_key.sql` ← `DERV_ACCT_PATY_99_SP_COMT_PROS_KEY.sql` (1.6KB, 58 lines)

### Account Balance
- `int_acct_baln_bkdt_adj_rule_isrt.sql` ← `ACCT_BALN_BKDT_ADJ_RULE_ISRT.sql` (4.9KB, 164 lines)
- `int_acct_baln_bkdt_audt_get_pros_key.sql` ← `ACCT_BALN_BKDT_AUDT_GET_PROS_KEY.sql` (5.4KB, 177 lines)
- `int_acct_baln_bkdt_audt_isrt.sql` ← `ACCT_BALN_BKDT_AUDT_ISRT.sql` (3.7KB, 114 lines)
- `int_acct_baln_bkdt_avg_call_proc.sql` ← `ACCT_BALN_BKDT_AVG_CALL_PROC.sql` (1.1KB, 35 lines)
- `int_acct_baln_bkdt_delt.sql` ← `ACCT_BALN_BKDT_DELT.sql` (1.8KB, 50 lines)
- `int_acct_baln_bkdt_get_pros_key.sql` ← `ACCT_BALN_BKDT_GET_PROS_KEY.sql` (5.3KB, 178 lines)
- `int_acct_baln_bkdt_isrt.sql` ← `ACCT_BALN_BKDT_ISRT.sql` (1.9KB, 67 lines)
- `int_acct_baln_bkdt_recn_get_pros_key.sql` ← `ACCT_BALN_BKDT_RECN_GET_PROS_KEY.sql` (5.2KB, 175 lines)
- `int_acct_baln_bkdt_recn_isrt.sql` ← `ACCT_BALN_BKDT_RECN_ISRT.sql` (5.0KB, 238 lines)
- `int_acct_baln_bkdt_stg_isrt.sql` ← `ACCT_BALN_BKDT_STG_ISRT.sql` (5.1KB, 174 lines)
- `int_acct_baln_bkdt_util_pros_updt.sql` ← `ACCT_BALN_BKDT_UTIL_PROS_UPDT.sql` (1.3KB, 48 lines)

### Process Control
- `int_sp_comt_btch_key.sql` ← `sp_comt_btch_key.sql` (1.2KB, 47 lines)
- `int_sp_comt_pros_key.sql` ← `sp_comt_pros_key.sql` (1.3KB, 52 lines)
- `int_sp_get_btch_key.sql` ← `sp_get_btch_key.sql` (1.5KB, 62 lines)
- `int_sp_get_pros_key.sql` ← `sp_get_pros_key.sql` (1.5KB, 59 lines)

### Portfolio Technical
- `int_prtf_tech_acct_int_grup_psst.sql` ← `prtf_tech_acct_int_grup_psst.sql` (7.8KB, 286 lines)
- `int_prtf_tech_acct_own_rel_psst.sql` ← `prtf_tech_acct_own_rel_psst.sql` (3.0KB, 106 lines)
- `int_prtf_tech_acct_psst.sql` ← `prtf_tech_acct_psst.sql` (7.4KB, 269 lines)
- `int_prtf_tech_acct_rel_psst.sql` ← `prtf_tech_acct_rel_psst.sql` (3.8KB, 123 lines)
- `int_prtf_tech_daly_datawatcher_c.sql` ← `prtf_tech_daly_datawatcher_c.sql` (2.6KB, 94 lines)
- `int_prtf_tech_grd_prtf_type_enhc_psst.sql` ← `prtf_tech_grd_prtf_type_enhc_psst.sql` (1.9KB, 80 lines)
- `int_prtf_tech_int_grup_enhc_psst.sql` ← `prtf_tech_int_grup_enhc_psst.sql` (5.9KB, 210 lines)
- `int_prtf_tech_int_grup_own_psst.sql` ← `prtf_tech_int_grup_own_psst.sql` (11.3KB, 328 lines)
- `int_prtf_tech_int_psst.sql` ← `prtf_tech_int_psst.sql` (2.2KB, 78 lines)
- `int_prtf_tech_own_psst.sql` ← `prtf_tech_own_psst.sql` (13.4KB, 406 lines)
- `int_prtf_tech_own_rel_psst.sql` ← `prtf_tech_own_rel_psst.sql` (3.9KB, 132 lines)
- `int_prtf_tech_paty_int_grup_psst.sql` ← `prtf_tech_paty_int_grup_psst.sql` (7.3KB, 252 lines)
- `int_prtf_tech_paty_own_rel_psst.sql` ← `prtf_tech_paty_own_rel_psst.sql` (3.1KB, 106 lines)
- `int_prtf_tech_paty_psst.sql` ← `prtf_tech_paty_psst.sql` (6.5KB, 228 lines)
- `int_prtf_tech_paty_rel_psst.sql` ← `prtf_tech_paty_rel_psst.sql` (3.7KB, 123 lines)

### Data Loading
- `int_bteq_sap_edo_wkly_load.sql` ← `BTEQ_SAP_EDO_WKLY_LOAD.sql` (20.9KB, 638 lines)
- `int_bteq_tax_inss_mnly_load.sql` ← `BTEQ_TAX_INSS_MNLY_LOAD.sql` (8.2KB, 320 lines)
- `int_bteq_login.sql` ← `bteq_login.sql` (0.0KB, 1 lines)

---

**Generated**: 2025-08-21 10:51:05  
**Converter Version**: 1.0  
**Total Conversion Time**: Automated
