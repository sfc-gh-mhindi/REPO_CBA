# DERV_ACCT_PATY_03_SET_ACCT_PRTF.sql - BTEQ Analysis

## File Overview
- **File Name**: DERV_ACCT_PATY_03_SET_ACCT_PRTF.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 12
- **SQL Blocks**: 4

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 37 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 38 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 51 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 53 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.DERV_PRTF_ACCT_STAG;` |
| 54 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 59 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 61 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 73 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 75 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.DERV_PRTF_PATY_STAG;` |
| 76 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 86 | LABEL | `.LABEL EXITERR` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 36-36)
- **Complexity Score**: 4
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
DELETE FROM %%DDSTG%%.DERV_PRTF_ACCT_STAG;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%DDSTG%%.DERV_PRTF_ACCT_STAG
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%ddstg%%"."derv_prtf_acct_stag"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_ACCT_STAG

### SQL Block 2 (Lines 41-50)
- **Complexity Score**: 26
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_PRTF_ACCT_STAG
 SELECT ACCT_I
        ,PRTF_CODE_X
   FROM  %%VTECH%%.DERV_PRTF_ACCT 
 
 WHERE PERD_D = :EXTR_D
   AND DERV_PRTF_CATG_C = 'RM'
    
 GROUP BY 1,2;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_PRTF_ACCT_STAG SELECT ACCT_I, PRTF_CODE_X FROM %%VTECH%%.DERV_PRTF_ACCT WHERE PERD_D = :EXTR_D AND DERV_PRTF_CATG_C = 'RM' GROUP BY 1, 2
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."derv_prtf_acct_stag"
SELECT
  "derv_prtf_acct"."acct_i" AS "acct_i",
  "derv_prtf_acct"."prtf_code_x" AS "prtf_code_x"
FROM "%%vtech%%"."derv_prtf_acct" AS "derv_prtf_acct"
WHERE
  "derv_prtf_acct"."derv_prtf_catg_c" = 'RM'
  AND "derv_prtf_acct"."perd_d" = :EXTR_D
GROUP BY
  "derv_prtf_acct"."acct_i",
  "derv_prtf_acct"."prtf_code_x"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_ACCT, DERV_PRTF_ACCT_STAG
- **Columns**: ACCT_I, DERV_PRTF_CATG_C, PERD_D, PRTF_CODE_X
- **Functions**: PERD_D = :EXTR_D

### SQL Block 3 (Lines 58-58)
- **Complexity Score**: 4
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
DELETE FROM %%DDSTG%%.DERV_PRTF_PATY_STAG;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%DDSTG%%.DERV_PRTF_PATY_STAG
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%ddstg%%"."derv_prtf_paty_stag"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_PATY_STAG

### SQL Block 4 (Lines 64-72)
- **Complexity Score**: 26
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_PRTF_PATY_STAG
 SELECT PATY_I
        ,PRTF_CODE_X
   FROM  %%VTECH%%.DERV_PRTF_PATY 
 
 WHERE PERD_D = :EXTR_D
   AND DERV_PRTF_CATG_C = 'RM'
 
 GROUP BY 1,2;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_PRTF_PATY_STAG SELECT PATY_I, PRTF_CODE_X FROM %%VTECH%%.DERV_PRTF_PATY WHERE PERD_D = :EXTR_D AND DERV_PRTF_CATG_C = 'RM' GROUP BY 1, 2
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."derv_prtf_paty_stag"
SELECT
  "derv_prtf_paty"."paty_i" AS "paty_i",
  "derv_prtf_paty"."prtf_code_x" AS "prtf_code_x"
FROM "%%vtech%%"."derv_prtf_paty" AS "derv_prtf_paty"
WHERE
  "derv_prtf_paty"."derv_prtf_catg_c" = 'RM'
  AND "derv_prtf_paty"."perd_d" = :EXTR_D
GROUP BY
  "derv_prtf_paty"."paty_i",
  "derv_prtf_paty"."prtf_code_x"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_PATY, DERV_PRTF_PATY_STAG
- **Columns**: DERV_PRTF_CATG_C, PATY_I, PERD_D, PRTF_CODE_X
- **Functions**: PERD_D = :EXTR_D

## Migration Recommendations

### Suggested Migration Strategy
**Medium complexity** - Incremental model with DCF hooks recommended

### DCF Mapping
- **Error Handling**: Convert `.IF ERRORCODE` patterns to DCF error handling macros
- **File Operations**: Replace IMPORT/EXPORT with dbt seeds or external tables
- **Statistics**: Replace COLLECT STATS with Snowflake post-hooks
- **Stored Procedures**: Map CALL statements to DCF stored procedure macros

---

*Generated by BTEQ Parser Service - 2025-08-20 12:39:19*
