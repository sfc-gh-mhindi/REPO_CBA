# BTEQ Migration Analysis Report

## Executive Summary

This report provides a comprehensive analysis of all BTEQ files in the current state, designed to inform the agentic migration solution for converting Teradata BTEQ scripts to dbt models using the DCF (dbt Control Framework).

## Analysis Overview


### File Processing Summary
- **Total Files Analyzed**: 49
- **Successfully Processed**: 47
- **Failed to Process**: 2
- **Total Control Statements**: 744
- **Total SQL Blocks**: 173


### Control Statement Distribution
| Control Type | Count | Purpose |
|--------------|-------|---------|
| CALL_SP | 5 | Call stored procedures |
| COLLECT_STATS | 54 | Collect table statistics |
| EXPORT | 22 | Export data to files |
| IF_ERRORCODE | 397 | Error handling and flow control |
| IMPORT | 45 | Import data from files |
| LABEL | 93 | Define jump targets |
| LOGOFF | 73 | Database disconnection |
| LOGON | 1 | Database connection |
| OS_CMD | 8 | Execute operating system commands |
| RUN | 46 | Execute external scripts/files |


### Teradata-Specific Features Requiring Migration Attention
| Feature | Occurrences | Migration Complexity |
|---------|-------------|---------------------|
| Variable substitution | 167 | Low - Replace with dbt variables |
| QUALIFY clause | 28 | Medium - Convert to window function + WHERE |
| ROW_NUMBER() OVER | 22 | Low - Direct Snowflake support |
| ADD_MONTHS function | 17 | Low - Use DATEADD in Snowflake |
| EXTRACT with complex syntax | 9 | Medium - Simplify extraction logic |
| YEAR(4) TO MONTH intervals | 1 | High - Complex date arithmetic conversion |


### SQL Complexity Analysis

The following metrics help determine the appropriate dbt materialization strategy:

| Metric | Min | Max | Avg | Total |
|--------|-----|-----|-----|-------|
| Total Nodes | 0 | 788 | 89.3 | 15452 |
| Select Count | 0 | 12 | 1.2 | 211 |
| Join Count | 0 | 7 | 0.7 | 128 |
| Subquery Count | 0 | 10 | 0.7 | 127 |
| Case Statements | 0 | 17 | 0.5 | 92 |
| Window Functions | 0 | 3 | 0.3 | 54 |
| Aggregate Functions | 0 | 4 | 0.3 | 58 |


## Detailed File Analysis

### Migration Recommendations by File


#### DERV_ACCT_PATY_07_CRAT_DELTAS.sql


- **Control Statements**: 17
- **SQL Blocks**: 6
- **Total Complexity Score**: 334
- **Teradata Features**: 1
- **Migration Strategy**: **High complexity** - Break into multiple models, use DCF full framework

**Teradata Features Detected**:
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 37: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
3. Line 40: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
4. Line 65: `IF_ERRORCODE` -   .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
5. Line 67: `COLLECT_STATS` -   COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_ADD;
6. Line 68: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
7. Line 74: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
8. Line 76: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
9. Line 104: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
10. Line 106: `COLLECT_STATS` -  COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CHG;
11. Line 107: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
12. Line 113: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
13. Line 115: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
14. Line 140: `IF_ERRORCODE` -   .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
15. Line 142: `COLLECT_STATS` -   COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_DEL;
16. Line 143: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
17. Line 151: `LABEL` - .LABEL EXITERR


#### DERV_ACCT_PATY_08_APPLY_DELTAS.sql


- **Control Statements**: 20
- **SQL Blocks**: 8
- **Total Complexity Score**: 340
- **Teradata Features**: 3
- **Migration Strategy**: **High complexity** - Break into multiple models, use DCF full framework

**Teradata Features Detected**:
- QUALIFY clause
- ROW_NUMBER() OVER
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 38: `OS_CMD` - .OS rm /cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_PROS_KE...
3. Line 42: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
4. Line 43: `EXPORT` - .EXPORT DATA FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_...
5. Line 55: `EXPORT` - .EXPORT RESET
6. Line 57: `IF_ERRORCODE` -  .IF ERRORCODE <> 0    THEN .GOTO EXITERR
7. Line 61: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
8. Line 82: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
9. Line 86: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
10. Line 110: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
11. Line 115: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
12. Line 153: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
13. Line 159: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
14. Line 185: `IF_ERRORCODE` -   .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
15. Line 191: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
16. Line 206: `IF_ERRORCODE` -   .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
17. Line 208: `COLLECT_STATS` -  COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_ROW_SECU_FIX;
18. Line 209: `IF_ERRORCODE` -   .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
19. Line 218: `IF_ERRORCODE` -   .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
20. Line 227: `LABEL` - .LABEL EXITERR


#### ACCT_BALN_BKDT_AUDT_GET_PROS_KEY.sql


- **Control Statements**: 6
- **SQL Blocks**: 1
- **Total Complexity Score**: 297
- **Teradata Features**: 1
- **Migration Strategy**: **High complexity** - Break into multiple models, use DCF full framework

**Teradata Features Detected**:
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
3. Line 168: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
4. Line 171: `LOGOFF` - .LOGOFF
5. Line 174: `LABEL` - .LABEL EXITERR
6. Line 176: `LOGOFF` - .LOGOFF


#### DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG.sql


- **Control Statements**: 19
- **SQL Blocks**: 7
- **Total Complexity Score**: 725
- **Teradata Features**: 3
- **Migration Strategy**: **High complexity** - Break into multiple models, use DCF full framework

**Teradata Features Detected**:
- QUALIFY clause
- ROW_NUMBER() OVER
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 39: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
3. Line 64: `IF_ERRORCODE` -    .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
4. Line 66: `COLLECT_STATS` -    COLLECT STATS    %%DDSTG%%.DERV_ACCT_PATY_NON_RM;
5. Line 67: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
6. Line 80: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
7. Line 128: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
8. Line 154: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
9. Line 163: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
10. Line 197: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
11. Line 199: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_FLAG;
12. Line 201: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
13. Line 213: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
14. Line 228: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
15. Line 231: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
16. Line 320: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
17. Line 322: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_FLAG;
18. Line 324: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
19. Line 332: `LABEL` - .LABEL EXITERR


#### ACCT_BALN_BKDT_AVG_CALL_PROC.sql


- **Control Statements**: 7
- **SQL Blocks**: 0
- **Total Complexity Score**: 0
- **Teradata Features**: 0
- **Migration Strategy**: **Control-only script** - Convert to dbt macro or pre/post-hook

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
3. Line 24: `CALL_SP` - CALL %%CAD_PROD_MACRO%%.SP_CALC_AVRG_DAY_BALN_BKDT (
CAST(AD...
4. Line 27: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
5. Line 30: `LOGOFF` - .LOGOFF
6. Line 32: `LABEL` - .LABEL EXITERR
7. Line 34: `LOGOFF` - .LOGOFF


#### DERV_ACCT_PATY_02_CRAT_WORK_TABL_CHG0379808.sql

❌ **FAILED TO PROCESS**: 'utf-8' codec can't decode byte 0x96 in position 22201: invalid start byte


#### sp_comt_btch_key.sql


- **Control Statements**: 8
- **SQL Blocks**: 1
- **Total Complexity Score**: 32
- **Teradata Features**: 1
- **Migration Strategy**: **Simple transformation** - Standard dbt model with table materialization

**Teradata Features Detected**:
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
3. Line 23: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
4. Line 36: `EXPORT` - .EXPORT RESET
5. Line 38: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
6. Line 41: `LOGOFF` - .LOGOFF
7. Line 43: `LABEL` - .LABEL EXITERR
8. Line 45: `LOGOFF` - .LOGOFF


#### prtf_tech_own_psst.sql


- **Control Statements**: 21
- **SQL Blocks**: 12
- **Total Complexity Score**: 1202
- **Teradata Features**: 5
- **Migration Strategy**: **High complexity** - Break into multiple models, use DCF full framework

**Teradata Features Detected**:
- ADD_MONTHS function
- EXTRACT with complex syntax
- QUALIFY clause
- ROW_NUMBER() OVER
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
3. Line 29: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
4. Line 84: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
5. Line 87: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
6. Line 94: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
7. Line 163: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
8. Line 166: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
9. Line 184: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
10. Line 208: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
11. Line 215: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
12. Line 270: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
13. Line 273: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
14. Line 283: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
15. Line 354: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
16. Line 357: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
17. Line 374: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
18. Line 398: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
19. Line 401: `LOGOFF` - .LOGOFF
20. Line 403: `LABEL` - .LABEL EXITERR
21. Line 405: `LOGOFF` - .LOGOFF


#### sp_get_btch_key.sql


- **Control Statements**: 13
- **SQL Blocks**: 1
- **Total Complexity Score**: 0
- **Teradata Features**: 1
- **Migration Strategy**: **Simple transformation** - Standard dbt model with table materialization

**Teradata Features Detected**:
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
3. Line 23: `EXPORT` - .EXPORT RESET
4. Line 26: `OS_CMD` - .OS rm /cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_BTCH_KE...
5. Line 28: `EXPORT` - .EXPORT DATA FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_...
6. Line 32: `CALL_SP` - CALL %%STARMACRDB%%.SP_GET_BTCH_KEY(     
  '%%SRCE_SYST_M%%...
7. Line 41: `EXPORT` - .EXPORT RESET
8. Line 44: `OS_CMD` - .OS rm /cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_DATE.tx...
9. Line 46: `EXPORT` - .EXPORT DATA FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_...
10. Line 53: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
11. Line 56: `LOGOFF` - .LOGOFF
12. Line 58: `LABEL` - .LABEL EXITERR
13. Line 60: `LOGOFF` - .LOGOFF


#### ACCT_BALN_BKDT_DELT.sql


- **Control Statements**: 6
- **SQL Blocks**: 1
- **Total Complexity Score**: 102
- **Teradata Features**: 1
- **Migration Strategy**: **Medium complexity** - Incremental model with DCF hooks recommended

**Teradata Features Detected**:
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
3. Line 41: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
4. Line 44: `LOGOFF` - .LOGOFF
5. Line 47: `LABEL` - .LABEL EXITERR
6. Line 49: `LOGOFF` - .LOGOFF


#### ACCT_BALN_BKDT_GET_PROS_KEY.sql


- **Control Statements**: 6
- **SQL Blocks**: 1
- **Total Complexity Score**: 307
- **Teradata Features**: 1
- **Migration Strategy**: **High complexity** - Break into multiple models, use DCF full framework

**Teradata Features Detected**:
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
3. Line 170: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
4. Line 173: `LOGOFF` - .LOGOFF
5. Line 175: `LABEL` - .LABEL EXITERR
6. Line 177: `LOGOFF` - .LOGOFF


#### prtf_tech_paty_own_rel_psst.sql


- **Control Statements**: 11
- **SQL Blocks**: 2
- **Total Complexity Score**: 165
- **Teradata Features**: 1
- **Migration Strategy**: **Medium complexity** - Incremental model with DCF hooks recommended

**Teradata Features Detected**:
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
3. Line 32: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
4. Line 36: `COLLECT_STATS` -  COLLECT STATS  %%STARDATADB%%.DERV_PRTF_PATY_OWN_REL;
5. Line 38: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
6. Line 92: `IF_ERRORCODE` -   .IF ERRORCODE <> 0    THEN .GOTO EXITERR
7. Line 94: `COLLECT_STATS` -   COLLECT STATS  %%STARDATADB%%.DERV_PRTF_PATY_OWN_REL;
8. Line 96: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
9. Line 101: `LOGOFF` - .LOGOFF
10. Line 103: `LABEL` - .LABEL EXITERR
11. Line 105: `LOGOFF` - .LOGOFF


#### prtf_tech_paty_psst.sql


- **Control Statements**: 12
- **SQL Blocks**: 6
- **Total Complexity Score**: 559
- **Teradata Features**: 5
- **Migration Strategy**: **High complexity** - Break into multiple models, use DCF full framework

**Teradata Features Detected**:
- ADD_MONTHS function
- EXTRACT with complex syntax
- QUALIFY clause
- ROW_NUMBER() OVER
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
3. Line 29: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
4. Line 100: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
5. Line 110: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
6. Line 170: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
7. Line 187: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
8. Line 215: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
9. Line 219: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
10. Line 223: `LOGOFF` - .LOGOFF
11. Line 225: `LABEL` - .LABEL EXITERR
12. Line 227: `LOGOFF` - .LOGOFF


#### prtf_tech_int_grup_enhc_psst.sql


- **Control Statements**: 15
- **SQL Blocks**: 6
- **Total Complexity Score**: 605
- **Teradata Features**: 5
- **Migration Strategy**: **High complexity** - Break into multiple models, use DCF full framework

**Teradata Features Detected**:
- ADD_MONTHS function
- EXTRACT with complex syntax
- QUALIFY clause
- ROW_NUMBER() OVER
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
3. Line 35: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
4. Line 63: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
5. Line 67: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
6. Line 76: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
7. Line 79: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
8. Line 131: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
9. Line 134: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
10. Line 141: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
11. Line 197: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
12. Line 201: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
13. Line 205: `LOGOFF` - .LOGOFF
14. Line 207: `LABEL` - .LABEL EXITERR
15. Line 209: `LOGOFF` - .LOGOFF


#### prtf_tech_acct_int_grup_psst.sql


- **Control Statements**: 16
- **SQL Blocks**: 8
- **Total Complexity Score**: 686
- **Teradata Features**: 5
- **Migration Strategy**: **High complexity** - Break into multiple models, use DCF full framework

**Teradata Features Detected**:
- ADD_MONTHS function
- EXTRACT with complex syntax
- QUALIFY clause
- ROW_NUMBER() OVER
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
3. Line 32: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
4. Line 69: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
5. Line 74: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
6. Line 82: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
7. Line 154: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
8. Line 158: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
9. Line 166: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
10. Line 224: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
11. Line 228: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
12. Line 248: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
13. Line 276: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
14. Line 280: `LOGOFF` - .LOGOFF
15. Line 282: `LABEL` - .LABEL EXITERR
16. Line 284: `LOGOFF` - .LOGOFF


#### prtf_tech_int_grup_own_psst.sql


- **Control Statements**: 16
- **SQL Blocks**: 8
- **Total Complexity Score**: 358
- **Teradata Features**: 5
- **Migration Strategy**: **High complexity** - Break into multiple models, use DCF full framework

**Teradata Features Detected**:
- ADD_MONTHS function
- EXTRACT with complex syntax
- QUALIFY clause
- ROW_NUMBER() OVER
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
3. Line 36: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
4. Line 106: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
5. Line 110: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
6. Line 118: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
7. Line 178: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
8. Line 181: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
9. Line 189: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
10. Line 275: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
11. Line 278: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
12. Line 296: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
13. Line 320: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
14. Line 323: `LOGOFF` - .LOGOFF
15. Line 325: `LABEL` - .LABEL EXITERR
16. Line 327: `LOGOFF` - .LOGOFF


#### DERV_ACCT_PATY_03_SET_ACCT_PRTF.sql


- **Control Statements**: 12
- **SQL Blocks**: 4
- **Total Complexity Score**: 60
- **Teradata Features**: 1
- **Migration Strategy**: **Medium complexity** - Incremental model with DCF hooks recommended

**Teradata Features Detected**:
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 37: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
3. Line 38: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
4. Line 51: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
5. Line 53: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.DERV_PRTF_ACCT_STAG;
6. Line 54: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
7. Line 59: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
8. Line 61: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
9. Line 73: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
10. Line 75: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.DERV_PRTF_PATY_STAG;
11. Line 76: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
12. Line 86: `LABEL` - .LABEL EXITERR


#### ACCT_BALN_BKDT_ISRT.sql


- **Control Statements**: 6
- **SQL Blocks**: 1
- **Total Complexity Score**: 52
- **Teradata Features**: 1
- **Migration Strategy**: **Medium complexity** - Incremental model with DCF hooks recommended

**Teradata Features Detected**:
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
3. Line 58: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
4. Line 61: `LOGOFF` - .LOGOFF
5. Line 64: `LABEL` - .LABEL EXITERR
6. Line 66: `LOGOFF` - .LOGOFF


#### prtf_tech_acct_psst.sql


- **Control Statements**: 12
- **SQL Blocks**: 6
- **Total Complexity Score**: 635
- **Teradata Features**: 5
- **Migration Strategy**: **High complexity** - Break into multiple models, use DCF full framework

**Teradata Features Detected**:
- ADD_MONTHS function
- EXTRACT with complex syntax
- QUALIFY clause
- ROW_NUMBER() OVER
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
3. Line 34: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
4. Line 111: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
5. Line 119: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
6. Line 199: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
7. Line 203: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
8. Line 229: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
9. Line 257: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
10. Line 264: `LOGOFF` - .LOGOFF
11. Line 266: `LABEL` - .LABEL EXITERR
12. Line 268: `LOGOFF` - .LOGOFF


#### DERV_ACCT_PATY_04_POP_CURR_TABL.sql


- **Control Statements**: 91
- **SQL Blocks**: 21
- **Total Complexity Score**: 4052
- **Teradata Features**: 3
- **Migration Strategy**: **High complexity** - Break into multiple models, use DCF full framework

**Teradata Features Detected**:
- QUALIFY clause
- ROW_NUMBER() OVER
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 49: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
3. Line 51: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.ACCT_PATY_DEDUP;
4. Line 52: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
5. Line 54: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
6. Line 79: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
7. Line 82: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.ACCT_PATY_DEDUP;
8. Line 83: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
9. Line 89: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
10. Line 91: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR;
11. Line 92: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
12. Line 94: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
13. Line 112: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
14. Line 116: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR;
15. Line 117: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
16. Line 129: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
17. Line 261: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
18. Line 265: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR;
19. Line 266: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
20. Line 275: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
21. Line 347: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
22. Line 351: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR;
23. Line 352: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
24. Line 362: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
25. Line 437: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
26. Line 439: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR;
27. Line 440: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
28. Line 444: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
29. Line 496: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
30. Line 500: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR;
31. Line 501: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
32. Line 505: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
33. Line 556: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
34. Line 560: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR;
35. Line 561: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
36. Line 566: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
37. Line 568: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_WSS;
38. Line 569: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
39. Line 571: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
40. Line 620: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
41. Line 623: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_WSS;
42. Line 624: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
43. Line 631: `IMPORT` -  .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%S...
44. Line 654: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
45. Line 657: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.ACCT_REL_WSS_DITPS;
46. Line 658: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
47. Line 667: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
48. Line 669: `COLLECT_STATS` -  COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_WSS;
49. Line 670: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
50. Line 675: `IMPORT` -  .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%S...
51. Line 761: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
52. Line 763: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_WSS;
53. Line 764: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
54. Line 779: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
55. Line 781: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR;
56. Line 782: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
57. Line 793: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
58. Line 795: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_THA;
59. Line 796: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
60. Line 798: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
61. Line 819: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
62. Line 821: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_THA;
63. Line 822: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
64. Line 825: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
65. Line 827: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE;
66. Line 828: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
67. Line 855: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
68. Line 857: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE;
69. Line 858: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
70. Line 877: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
71. Line 881: `COLLECT_STATS` -  COLLECT STATS %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE;
72. Line 882: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
73. Line 885: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
74. Line 887: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_THA;
75. Line 888: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
76. Line 900: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
77. Line 904: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_THA;
78. Line 905: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
79. Line 907: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
80. Line 958: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
81. Line 961: `COLLECT_STATS` -  COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR;
82. Line 962: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
83. Line 973: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
84. Line 1058: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
85. Line 1061: `COLLECT_STATS` -  COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR;
86. Line 1062: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
87. Line 1074: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
88. Line 1242: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
89. Line 1244: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR;
90. Line 1245: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
91. Line 1253: `LABEL` - .LABEL EXITERR


#### DERV_ACCT_PATY_99_DROP_WORK_TABL_CHG0379808.sql


- **Control Statements**: 64
- **SQL Blocks**: 0
- **Total Complexity Score**: 0
- **Teradata Features**: 0
- **Migration Strategy**: **Control-only script** - Convert to dbt macro or pre/post-hook

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 37: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
3. Line 46: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT1
4. Line 47: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT1
5. Line 50: `LABEL` - .LABEL NEXT1
6. Line 54: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT2
7. Line 55: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT2
8. Line 58: `LABEL` - .LABEL NEXT2
9. Line 64: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT3
10. Line 65: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT3
11. Line 68: `LABEL` - .LABEL NEXT3
12. Line 71: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT4
13. Line 72: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT4
14. Line 75: `LABEL` - .LABEL NEXT4
15. Line 78: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT5
16. Line 79: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT5
17. Line 82: `LABEL` - .LABEL NEXT5
18. Line 85: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT6
19. Line 86: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT6
20. Line 89: `LABEL` - .LABEL NEXT6
21. Line 93: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT7
22. Line 94: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT7
23. Line 97: `LABEL` - .LABEL NEXT7
24. Line 102: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT8
25. Line 103: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT8
26. Line 106: `LABEL` - .LABEL NEXT8
27. Line 109: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT9
28. Line 110: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT9
29. Line 113: `LABEL` - .LABEL NEXT9
30. Line 116: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT10
31. Line 117: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT10
32. Line 120: `LABEL` - .LABEL NEXT10
33. Line 124: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT11
34. Line 125: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT11
35. Line 128: `LABEL` - .LABEL NEXT11
36. Line 132: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT12
37. Line 133: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT12
38. Line 136: `LABEL` - .LABEL NEXT12
39. Line 142: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT13
40. Line 143: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT13
41. Line 146: `LABEL` - .LABEL NEXT13
42. Line 149: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT14
43. Line 150: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT14
44. Line 153: `LABEL` - .LABEL NEXT14
45. Line 156: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT15
46. Line 157: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT15
47. Line 160: `LABEL` - .LABEL NEXT15
48. Line 163: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT16
49. Line 164: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT16
50. Line 167: `LABEL` - .LABEL NEXT16
51. Line 172: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT17
52. Line 173: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT17
53. Line 176: `LABEL` - .LABEL NEXT17
54. Line 180: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT18
55. Line 181: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT18
56. Line 184: `LABEL` - .LABEL NEXT18
57. Line 187: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT19
58. Line 188: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT19
59. Line 191: `LABEL` - .LABEL NEXT19
60. Line 194: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT20
61. Line 195: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT20
62. Line 198: `LABEL` - .LABEL NEXT20
63. Line 200: `LABEL` - .LABEL EXITOK
64. Line 207: `LABEL` - .LABEL EXITERR


#### sp_comt_pros_key.sql


- **Control Statements**: 8
- **SQL Blocks**: 1
- **Total Complexity Score**: 54
- **Teradata Features**: 1
- **Migration Strategy**: **Medium complexity** - Incremental model with DCF hooks recommended

**Teradata Features Detected**:
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
3. Line 23: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
4. Line 41: `EXPORT` - .EXPORT RESET
5. Line 43: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
6. Line 46: `LOGOFF` - .LOGOFF
7. Line 48: `LABEL` - .LABEL EXITERR
8. Line 50: `LOGOFF` - .LOGOFF


#### sp_get_pros_key.sql


- **Control Statements**: 12
- **SQL Blocks**: 0
- **Total Complexity Score**: 0
- **Teradata Features**: 0
- **Migration Strategy**: **Control-only script** - Convert to dbt macro or pre/post-hook

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
3. Line 23: `EXPORT` - .EXPORT RESET
4. Line 26: `OS_CMD` - .OS rm /cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_%%TBSHO...
5. Line 28: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
6. Line 30: `EXPORT` - .EXPORT DATA FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_...
7. Line 36: `CALL_SP` - CALL %%STARMACRDB%%.SP_GET_PROS_KEY(        
  '%%GDW_USER%%...
8. Line 48: `EXPORT` - .EXPORT RESET
9. Line 50: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
10. Line 53: `LOGOFF` - .LOGOFF
11. Line 55: `LABEL` - .LABEL EXITERR
12. Line 57: `LOGOFF` - .LOGOFF


#### prtf_tech_int_psst.sql


- **Control Statements**: 8
- **SQL Blocks**: 2
- **Total Complexity Score**: 82
- **Teradata Features**: 1
- **Migration Strategy**: **Medium complexity** - Incremental model with DCF hooks recommended

**Teradata Features Detected**:
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
3. Line 39: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
4. Line 61: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
5. Line 64: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
6. Line 68: `LOGOFF` - .LOGOFF
7. Line 70: `LABEL` - .LABEL EXITERR
8. Line 72: `LOGOFF` - .LOGOFF


#### DERV_ACCT_PATY_02_CRAT_WORK_TABL.sql

❌ **FAILED TO PROCESS**: 'utf-8' codec can't decode byte 0x96 in position 22523: invalid start byte


#### prtf_tech_paty_rel_psst.sql


- **Control Statements**: 11
- **SQL Blocks**: 2
- **Total Complexity Score**: 236
- **Teradata Features**: 1
- **Migration Strategy**: **High complexity** - Break into multiple models, use DCF full framework

**Teradata Features Detected**:
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
3. Line 33: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
4. Line 37: `COLLECT_STATS` -  COLLECT STATS  %%STARDATADB%%.DERV_PRTF_PATY_REL;
5. Line 39: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
6. Line 110: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
7. Line 112: `COLLECT_STATS` - COLLECT STATS  %%STARDATADB%%.DERV_PRTF_PATY_REL;
8. Line 114: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
9. Line 118: `LOGOFF` - .LOGOFF
10. Line 120: `LABEL` - .LABEL EXITERR
11. Line 122: `LOGOFF` - .LOGOFF


#### ACCT_BALN_BKDT_UTIL_PROS_UPDT.sql


- **Control Statements**: 6
- **SQL Blocks**: 1
- **Total Complexity Score**: 0
- **Teradata Features**: 1
- **Migration Strategy**: **Simple transformation** - Standard dbt model with table materialization

**Teradata Features Detected**:
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` -  .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
3. Line 39: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
4. Line 42: `LOGOFF` - .LOGOFF
5. Line 45: `LABEL` - .LABEL EXITERR
6. Line 47: `LOGOFF` - .LOGOFF


#### prtf_tech_own_rel_psst.sql


- **Control Statements**: 11
- **SQL Blocks**: 2
- **Total Complexity Score**: 276
- **Teradata Features**: 1
- **Migration Strategy**: **High complexity** - Break into multiple models, use DCF full framework

**Teradata Features Detected**:
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
3. Line 33: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
4. Line 37: `COLLECT_STATS` - COLLECT STATS  %%STARDATADB%%.DERV_PRTF_OWN_REL;
5. Line 40: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
6. Line 120: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
7. Line 122: `COLLECT_STATS` - COLLECT STATS  %%STARDATADB%%.DERV_PRTF_OWN_REL;
8. Line 124: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
9. Line 127: `LOGOFF` - .LOGOFF
10. Line 129: `LABEL` - .LABEL EXITERR
11. Line 131: `LOGOFF` - .LOGOFF


#### bteq_login.sql


- **Control Statements**: 1
- **SQL Blocks**: 0
- **Total Complexity Score**: 0
- **Teradata Features**: 0
- **Migration Strategy**: **Control-only script** - Convert to dbt macro or pre/post-hook

**Control Flow**:
1. Line 1: `LOGON` - .logon %%GDW_HOST%%/%%GDW_USER%%,%%GDW_PASS%%;


#### DERV_ACCT_PATY_99_DROP_WORK_TABL.sql


- **Control Statements**: 70
- **SQL Blocks**: 0
- **Total Complexity Score**: 0
- **Teradata Features**: 0
- **Migration Strategy**: **Control-only script** - Convert to dbt macro or pre/post-hook

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 39: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
3. Line 48: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT1
4. Line 49: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT1
5. Line 52: `LABEL` - .LABEL NEXT1
6. Line 56: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT2
7. Line 57: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT2
8. Line 60: `LABEL` - .LABEL NEXT2
9. Line 66: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT3
10. Line 67: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT3
11. Line 70: `LABEL` - .LABEL NEXT3
12. Line 73: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT4
13. Line 74: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT4
14. Line 77: `LABEL` - .LABEL NEXT4
15. Line 80: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT5
16. Line 81: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT5
17. Line 84: `LABEL` - .LABEL NEXT5
18. Line 87: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT6
19. Line 88: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT6
20. Line 91: `LABEL` - .LABEL NEXT6
21. Line 95: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT7
22. Line 96: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT7
23. Line 99: `LABEL` - .LABEL NEXT7
24. Line 104: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT8
25. Line 105: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT8
26. Line 108: `LABEL` - .LABEL NEXT8
27. Line 111: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT9
28. Line 112: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT9
29. Line 115: `LABEL` - .LABEL NEXT9
30. Line 118: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT10
31. Line 119: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT10
32. Line 122: `LABEL` - .LABEL NEXT10
33. Line 126: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT11
34. Line 127: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT11
35. Line 130: `LABEL` - .LABEL NEXT11
36. Line 134: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT12
37. Line 135: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT12
38. Line 138: `LABEL` - .LABEL NEXT12
39. Line 144: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT13
40. Line 145: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT13
41. Line 148: `LABEL` - .LABEL NEXT13
42. Line 151: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT14
43. Line 152: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT14
44. Line 155: `LABEL` - .LABEL NEXT14
45. Line 158: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT15
46. Line 159: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT15
47. Line 162: `LABEL` - .LABEL NEXT15
48. Line 165: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT16
49. Line 166: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT16
50. Line 169: `LABEL` - .LABEL NEXT16
51. Line 174: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT17
52. Line 175: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT17
53. Line 178: `LABEL` - .LABEL NEXT17
54. Line 182: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT18
55. Line 183: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT18
56. Line 186: `LABEL` - .LABEL NEXT18
57. Line 189: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT19
58. Line 190: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT19
59. Line 193: `LABEL` - .LABEL NEXT19
60. Line 196: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT20
61. Line 197: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT20
62. Line 200: `LABEL` - .LABEL NEXT20
63. Line 204: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT21
64. Line 205: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT21
65. Line 208: `LABEL` - .LABEL NEXT21
66. Line 212: `IF_ERRORCODE` - .IF ERRORCODE = 3807 THEN .GOTO NEXT22
67. Line 213: `IF_ERRORCODE` - .IF ERRORCODE = 0    THEN .GOTO NEXT22
68. Line 216: `LABEL` - .LABEL NEXT22
69. Line 218: `LABEL` - .LABEL EXITOK
70. Line 225: `LABEL` - .LABEL EXITERR


#### DERV_ACCT_PATY_00_DATAWATCHER.sql


- **Control Statements**: 12
- **SQL Blocks**: 2
- **Total Complexity Score**: 257
- **Teradata Features**: 2
- **Migration Strategy**: **High complexity** - Break into multiple models, use DCF full framework

**Teradata Features Detected**:
- QUALIFY clause
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .run file=%%BTEQ_LOGON_SCRIPT%%
2. Line 44: `OS_CMD` - .OS rm /cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_da...
3. Line 51: `EXPORT` - .EXPORT DATA FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_...
4. Line 63: `EXPORT` - .EXPORT RESET
5. Line 66: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
6. Line 75: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
7. Line 158: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
8. Line 163: `LOGOFF` - .LOGOFF
9. Line 165: `LABEL` - .LABEL EXITERR
10. Line 167: `LOGOFF` - .LOGOFF
11. Line 169: `LABEL` - .LABEL REPOLL
12. Line 171: `LOGOFF` - .LOGOFF


#### ACCT_BALN_BKDT_AUDT_ISRT.sql


- **Control Statements**: 7
- **SQL Blocks**: 2
- **Total Complexity Score**: 195
- **Teradata Features**: 1
- **Migration Strategy**: **Medium complexity** - Incremental model with DCF hooks recommended

**Teradata Features Detected**:
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
3. Line 90: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
4. Line 105: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
5. Line 108: `LOGOFF` - .LOGOFF
6. Line 111: `LABEL` - .LABEL EXITERR
7. Line 113: `LOGOFF` - .LOGOFF


#### DERV_ACCT_PATY_99_SP_COMT_PROS_KEY.sql


- **Control Statements**: 8
- **SQL Blocks**: 1
- **Total Complexity Score**: 0
- **Teradata Features**: 1
- **Migration Strategy**: **Simple transformation** - Standard dbt model with table materialization

**Teradata Features Detected**:
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
3. Line 27: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
4. Line 47: `EXPORT` - .EXPORT RESET
5. Line 49: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
6. Line 52: `LOGOFF` - .LOGOFF
7. Line 54: `LABEL` - .LABEL EXITERR
8. Line 56: `LOGOFF` - .LOGOFF


#### prtf_tech_grd_prtf_type_enhc_psst.sql


- **Control Statements**: 12
- **SQL Blocks**: 4
- **Total Complexity Score**: 88
- **Teradata Features**: 1
- **Migration Strategy**: **Medium complexity** - Incremental model with DCF hooks recommended

**Teradata Features Detected**:
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
3. Line 22: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
4. Line 25: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
5. Line 39: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
6. Line 42: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
7. Line 49: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
8. Line 65: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
9. Line 68: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
10. Line 74: `LOGOFF` - .LOGOFF
11. Line 76: `LABEL` - .LABEL EXITERR
12. Line 78: `LOGOFF` - .LOGOFF


#### BTEQ_SAP_EDO_WKLY_LOAD.sql


- **Control Statements**: 32
- **SQL Blocks**: 11
- **Total Complexity Score**: 784
- **Teradata Features**: 2
- **Migration Strategy**: **High complexity** - Break into multiple models, use DCF full framework

**Teradata Features Detected**:
- QUALIFY clause
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
3. Line 75: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
4. Line 100: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
5. Line 119: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
6. Line 141: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
7. Line 161: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
8. Line 183: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
9. Line 234: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
10. Line 316: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
11. Line 331: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
12. Line 335: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
13. Line 405: `IF_ERRORCODE` - 	.IF ERRORCODE <> 0 THEN .GOTO EXITERR
14. Line 429: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
15. Line 435: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
16. Line 489: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
17. Line 543: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
18. Line 545: `COLLECT_STATS` - COLLECT STATS ON  %%STARDATADB%%.ACCT_REL;
19. Line 547: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
20. Line 602: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
21. Line 604: `COLLECT_STATS` - COLLECT STATS ON %%STARDATADB%%.UTIL_PROS_ISAC;
22. Line 605: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
23. Line 608: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
24. Line 611: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
25. Line 614: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
26. Line 617: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
27. Line 620: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
28. Line 623: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
29. Line 626: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
30. Line 629: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
31. Line 632: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
32. Line 637: `LABEL` - .LABEL EXITERR


#### prtf_tech_acct_own_rel_psst.sql


- **Control Statements**: 11
- **SQL Blocks**: 2
- **Total Complexity Score**: 165
- **Teradata Features**: 1
- **Migration Strategy**: **Medium complexity** - Incremental model with DCF hooks recommended

**Teradata Features Detected**:
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
3. Line 32: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
4. Line 36: `COLLECT_STATS` -  COLLECT STATS  %%STARDATADB%%.DERV_PRTF_ACCT_OWN_REL;
5. Line 38: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
6. Line 92: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
7. Line 94: `COLLECT_STATS` - COLLECT STATS  %%STARDATADB%%.DERV_PRTF_ACCT_OWN_REL;
8. Line 96: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
9. Line 101: `LOGOFF` - .LOGOFF
10. Line 103: `LABEL` - .LABEL EXITERR
11. Line 105: `LOGOFF` - .LOGOFF


#### prtf_tech_paty_int_grup_psst.sql


- **Control Statements**: 16
- **SQL Blocks**: 8
- **Total Complexity Score**: 681
- **Teradata Features**: 5
- **Migration Strategy**: **High complexity** - Break into multiple models, use DCF full framework

**Teradata Features Detected**:
- ADD_MONTHS function
- EXTRACT with complex syntax
- QUALIFY clause
- ROW_NUMBER() OVER
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
3. Line 33: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
4. Line 62: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
5. Line 66: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
6. Line 74: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
7. Line 123: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
8. Line 126: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
9. Line 134: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
10. Line 195: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
11. Line 198: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
12. Line 215: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
13. Line 244: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
14. Line 247: `LOGOFF` - .LOGOFF
15. Line 249: `LABEL` - .LABEL EXITERR
16. Line 251: `LOGOFF` - .LOGOFF


#### DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG_CHG0379808.sql


- **Control Statements**: 13
- **SQL Blocks**: 5
- **Total Complexity Score**: 305
- **Teradata Features**: 3
- **Migration Strategy**: **High complexity** - Break into multiple models, use DCF full framework

**Teradata Features Detected**:
- QUALIFY clause
- ROW_NUMBER() OVER
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 38: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
3. Line 63: `IF_ERRORCODE` -    .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
4. Line 65: `COLLECT_STATS` -    COLLECT STATS    %%DDSTG%%.DERV_ACCT_PATY_NON_RM;
5. Line 66: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
6. Line 79: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
7. Line 127: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
8. Line 153: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
9. Line 162: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
10. Line 196: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
11. Line 198: `COLLECT_STATS` - COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_FLAG;
12. Line 200: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
13. Line 207: `LABEL` - .LABEL EXITERR


#### DERV_ACCT_PATY_01_SP_GET_BTCH_KEY.sql


- **Control Statements**: 12
- **SQL Blocks**: 0
- **Total Complexity Score**: 0
- **Teradata Features**: 0
- **Migration Strategy**: **Control-only script** - Convert to dbt macro or pre/post-hook

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
3. Line 28: `EXPORT` - .EXPORT RESET
4. Line 31: `OS_CMD` - .OS rm /cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_BTCH_KE...
5. Line 33: `EXPORT` - .EXPORT DATA FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_...
6. Line 40: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
7. Line 43: `CALL_SP` - CALL %%STARMACRDB%%.SP_GET_BTCH_KEY(     
  '%%SRCE_SYST_M%%...
8. Line 52: `EXPORT` - .EXPORT RESET
9. Line 54: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
10. Line 57: `LOGOFF` - .LOGOFF
11. Line 59: `LABEL` - .LABEL EXITERR
12. Line 61: `LOGOFF` - .LOGOFF


#### ACCT_BALN_BKDT_RECN_GET_PROS_KEY.sql


- **Control Statements**: 6
- **SQL Blocks**: 1
- **Total Complexity Score**: 297
- **Teradata Features**: 1
- **Migration Strategy**: **High complexity** - Break into multiple models, use DCF full framework

**Teradata Features Detected**:
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
3. Line 164: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
4. Line 168: `LOGOFF` - .LOGOFF
5. Line 171: `LABEL` - .LABEL EXITERR
6. Line 173: `LOGOFF` - .LOGOFF


#### ACCT_BALN_BKDT_STG_ISRT.sql


- **Control Statements**: 9
- **SQL Blocks**: 4
- **Total Complexity Score**: 412
- **Teradata Features**: 2
- **Migration Strategy**: **High complexity** - Break into multiple models, use DCF full framework

**Teradata Features Detected**:
- QUALIFY clause
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
3. Line 28: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
4. Line 81: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
5. Line 88: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
6. Line 164: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
7. Line 167: `LOGOFF` - .LOGOFF
8. Line 171: `LABEL` - .LABEL EXITERR
9. Line 173: `LOGOFF` - .LOGOFF


#### ACCT_BALN_BKDT_RECN_ISRT.sql


- **Control Statements**: 11
- **SQL Blocks**: 5
- **Total Complexity Score**: 57
- **Teradata Features**: 1
- **Migration Strategy**: **Medium complexity** - Incremental model with DCF hooks recommended

**Teradata Features Detected**:
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
3. Line 25: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
4. Line 111: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
5. Line 199: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
6. Line 213: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
7. Line 216: `LOGOFF` - .LOGOFF
8. Line 219: `LABEL` - .LABEL EXITERR
9. Line 221: `LOGOFF` - .LOGOFF
10. Line 225: `LABEL` - .LABEL ERR_SEV
11. Line 236: `LOGOFF` - .LOGOFF


#### DERV_ACCT_PATY_05_SET_PRTF_PRFR_FLAG.sql


- **Control Statements**: 21
- **SQL Blocks**: 6
- **Total Complexity Score**: 381
- **Teradata Features**: 3
- **Migration Strategy**: **High complexity** - Break into multiple models, use DCF full framework

**Teradata Features Detected**:
- QUALIFY clause
- ROW_NUMBER() OVER
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 42: `IF_ERRORCODE` - .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
3. Line 44: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
4. Line 63: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
5. Line 65: `COLLECT_STATS` -  COLLECT STATS %%DDSTG%%.DERV_PRTF_ACCT_PATY_PSST;
6. Line 66: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
7. Line 79: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
8. Line 154: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
9. Line 156: `COLLECT_STATS` -  COLLECT STATS %%DDSTG%%.DERV_PRTF_ACCT_PATY_STAG;
10. Line 157: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
11. Line 213: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
12. Line 215: `COLLECT_STATS` -  COLLECT STATS %%DDSTG%%.DERV_PRTF_ACCT_PATY_STAG;
13. Line 216: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
14. Line 227: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
15. Line 229: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
16. Line 250: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
17. Line 252: `COLLECT_STATS` - COLLECT STATS    %%DDSTG%%.DERV_ACCT_PATY_RM;
18. Line 253: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
19. Line 259: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
20. Line 290: `IF_ERRORCODE` -  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
21. Line 300: `LABEL` - .LABEL EXITERR


#### prtf_tech_acct_rel_psst.sql


- **Control Statements**: 11
- **SQL Blocks**: 2
- **Total Complexity Score**: 236
- **Teradata Features**: 1
- **Migration Strategy**: **High complexity** - Break into multiple models, use DCF full framework

**Teradata Features Detected**:
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
3. Line 34: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
4. Line 38: `COLLECT_STATS` -  COLLECT STATS  %%STARDATADB%%.DERV_PRTF_ACCT_REL;
5. Line 40: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
6. Line 110: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
7. Line 112: `COLLECT_STATS` - COLLECT STATS  %%STARDATADB%%.DERV_PRTF_ACCT_REL;
8. Line 114: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
9. Line 118: `LOGOFF` - .LOGOFF
10. Line 120: `LABEL` - .LABEL EXITERR
11. Line 122: `LOGOFF` - .LOGOFF


#### DERV_ACCT_PATY_01_SP_GET_PROS_KEY.sql


- **Control Statements**: 16
- **SQL Blocks**: 1
- **Total Complexity Score**: 24
- **Teradata Features**: 1
- **Migration Strategy**: **Simple transformation** - Standard dbt model with table materialization

**Teradata Features Detected**:
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
3. Line 31: `OS_CMD` - .OS rm /cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_PROS_KE...
4. Line 32: `OS_CMD` - .OS rm /cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_BTCH_DA...
5. Line 36: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
6. Line 37: `EXPORT` - .EXPORT DATA FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_...
7. Line 49: `EXPORT` - .EXPORT RESET
8. Line 51: `IF_ERRORCODE` -  .IF ERRORCODE <> 0    THEN .GOTO EXITERR
9. Line 56: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
10. Line 57: `EXPORT` - .EXPORT DATA FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_...
11. Line 65: `CALL_SP` - CALL %%STARMACRDB%%.SP_GET_PROS_KEY(        
  '%%GDW_USER%%...
12. Line 77: `EXPORT` - .EXPORT RESET
13. Line 79: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
14. Line 82: `LOGOFF` - .LOGOFF
15. Line 84: `LABEL` - .LABEL EXITERR
16. Line 86: `LOGOFF` - .LOGOFF


#### ACCT_BALN_BKDT_ADJ_RULE_ISRT.sql


- **Control Statements**: 7
- **SQL Blocks**: 2
- **Total Complexity Score**: 4
- **Teradata Features**: 6
- **Migration Strategy**: **Simple transformation** - Standard dbt model with table materialization

**Teradata Features Detected**:
- ADD_MONTHS function
- EXTRACT with complex syntax
- QUALIFY clause
- ROW_NUMBER() OVER
- Variable substitution
- YEAR(4) TO MONTH intervals

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
3. Line 27: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
4. Line 156: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
5. Line 159: `LOGOFF` - .LOGOFF
6. Line 161: `LABEL` - .LABEL EXITERR
7. Line 163: `LOGOFF` - .LOGOFF


#### DERV_ACCT_PATY_99_SP_COMT_BTCH_KEY.sql


- **Control Statements**: 8
- **SQL Blocks**: 1
- **Total Complexity Score**: 32
- **Teradata Features**: 1
- **Migration Strategy**: **Simple transformation** - Standard dbt model with table materialization

**Teradata Features Detected**:
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
3. Line 23: `IMPORT` - .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%ST...
4. Line 36: `EXPORT` - .EXPORT RESET
5. Line 38: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
6. Line 41: `LOGOFF` - .LOGOFF
7. Line 43: `LABEL` - .LABEL EXITERR
8. Line 45: `LOGOFF` - .LOGOFF


#### prtf_tech_daly_datawatcher_c.sql


- **Control Statements**: 8
- **SQL Blocks**: 1
- **Total Complexity Score**: 143
- **Teradata Features**: 2
- **Migration Strategy**: **Medium complexity** - Incremental model with DCF hooks recommended

**Teradata Features Detected**:
- QUALIFY clause
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
3. Line 79: `IF_ERRORCODE` - .IF ERRORCODE <> 0    THEN .GOTO EXITERR
4. Line 83: `LOGOFF` - .LOGOFF
5. Line 85: `LABEL` - .LABEL EXITERR
6. Line 87: `LOGOFF` - .LOGOFF
7. Line 89: `LABEL` - .LABEL REPOLL
8. Line 91: `LOGOFF` - .LOGOFF


#### BTEQ_TAX_INSS_MNLY_LOAD.sql


- **Control Statements**: 19
- **SQL Blocks**: 7
- **Total Complexity Score**: 232
- **Teradata Features**: 3
- **Migration Strategy**: **High complexity** - Break into multiple models, use DCF full framework

**Teradata Features Detected**:
- QUALIFY clause
- ROW_NUMBER() OVER
- Variable substitution

**Control Flow**:
1. Line 1: `RUN` - .RUN FILE=%%BTEQ_LOGON_SCRIPT%%
2. Line 2: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
3. Line 42: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
4. Line 166: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
5. Line 176: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
6. Line 203: `IF_ERRORCODE` -  .IF ERRORCODE <> 0 THEN .GOTO EXITERR
7. Line 207: `IF_ERRORCODE` -  .IF ERRORCODE <> 0 THEN .GOTO EXITERR
8. Line 211: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
9. Line 241: `IF_ERRORCODE` -  .IF ERRORCODE <> 0 THEN .GOTO EXITERR
10. Line 243: `COLLECT_STATS` - COLLECT STATS ON %%STARDATADB%%.ACCT_PATY_TAX_INSS;
11. Line 245: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
12. Line 299: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
13. Line 301: `COLLECT_STATS` - COLLECT STATS ON %%STARDATADB%%.UTIL_PROS_ISAC;
14. Line 302: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
15. Line 305: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
16. Line 308: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
17. Line 311: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
18. Line 314: `IF_ERRORCODE` - .IF ERRORCODE <> 0 THEN .GOTO EXITERR
19. Line 319: `LABEL` - .LABEL EXITERR



## DCF (dbt Control Framework) Mapping Recommendations

Based on the analysis, here are the recommended DCF patterns for migration:

### 1. Error Handling Patterns
- **BTEQ `.IF ERRORCODE`** → **DCF error handling macros**
- **BTEQ `.GOTO EXITERR`** → **DCF `check_error_and_end_prcs` macro**

### 2. Data Loading Patterns
- **Simple INSERT statements** → **dbt models with `table` materialization**
- **Complex INSERT with business logic** → **dbt models with `incremental` materialization + DCF hooks**
- **DELETE statements** → **Pre-hooks or separate dbt operations**

### 3. Stored Procedure Calls
- **CALL statements** → **DCF stored procedure macros or post-hooks**

### 4. File Operations
- **IMPORT/EXPORT** → **dbt seeds or external table references**

### 5. Statistics Collection
- **COLLECT STATS** → **Post-hooks with Snowflake ANALYZE TABLE equivalents**

## Next Steps for Agentic Solution

1. **Pattern Classification**: Use this analysis to train the classifier service on BTEQ patterns
2. **Template Generation**: Create dbt model templates based on complexity and feature analysis
3. **DCF Integration**: Map identified patterns to appropriate DCF macros and materializations
4. **Validation Rules**: Implement checks for successful conversion of identified Teradata features

---

*This report was generated automatically by the BTEQ Parser Service with advanced SQLGlot analysis.*
