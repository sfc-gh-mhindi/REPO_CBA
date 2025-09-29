# DERV_ACCT_PATY_07_CRAT_DELTAS.sql - BTEQ Analysis

## File Overview
- **File Name**: DERV_ACCT_PATY_07_CRAT_DELTAS.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 17
- **SQL Blocks**: 6

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 37 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 40 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 65 | IF_ERRORCODE | `  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 67 | COLLECT_STATS | `  COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_ADD;` |
| 68 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 74 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 76 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 104 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 106 | COLLECT_STATS | ` COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CHG;` |
| 107 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 113 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 115 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 140 | IF_ERRORCODE | `  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 142 | COLLECT_STATS | `  COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_DEL;` |
| 143 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 151 | LABEL | `.LABEL EXITERR` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 36-36)
- **Complexity Score**: 4
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
DELETE FROM %%DDSTG%%.DERV_ACCT_PATY_ADD;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%DDSTG%%.DERV_ACCT_PATY_ADD
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%ddstg%%"."derv_acct_paty_add"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_ACCT_PATY_ADD

### SQL Block 2 (Lines 43-64)
- **Complexity Score**: 101
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_ADD
SELECT T1.ACCT_I
      ,T1.PATY_I
      ,T1.ASSC_ACCT_I
      ,T1.PATY_ACCT_REL_C
      ,T1.PRFR_PATY_F
      ,T1.SRCE_SYST_C
      ,T1.EFFT_D
      ,T1.EXPY_D
      ,T1.ROW_SECU_ACCS_C
FROM %%DDSTG%%.DERV_ACCT_PATY_FLAG T1
LEFT JOIN %%VTECH%%.DERV_ACCT_PATY T2

   ON T1.ACCT_I = T2.ACCT_I
  AND T1.PATY_I = T2.PATY_I
  AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
  AND :EXTR_D BETWEEN T2.EFFT_D AND T2.EXPY_D
  
  WHERE :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D 
    AND T2.ACCT_I IS NULL
  GROUP BY 1,2,3,4,5,6,7,8,9;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_ADD SELECT T1.ACCT_I, T1.PATY_I, T1.ASSC_ACCT_I, T1.PATY_ACCT_REL_C, T1.PRFR_PATY_F, T1.SRCE_SYST_C, T1.EFFT_D, T1.EXPY_D, T1.ROW_SECU_ACCS_C FROM %%DDSTG%%.DERV_ACCT_PATY_FLAG AS T1 LEFT JOIN %%VTECH%%.DERV_ACCT_PATY AS T2 ON T1.ACCT_I = T2.ACCT_I AND T1.PATY_I = T2.PATY_I AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C AND :EXTR_D BETWEEN T2.EFFT_D AND T2.EXPY_D WHERE :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D AND T2.ACCT_I IS NULL GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."derv_acct_paty_add"
SELECT
  "t1"."acct_i" AS "acct_i",
  "t1"."paty_i" AS "paty_i",
  "t1"."assc_acct_i" AS "assc_acct_i",
  "t1"."paty_acct_rel_c" AS "paty_acct_rel_c",
  "t1"."prfr_paty_f" AS "prfr_paty_f",
  "t1"."srce_syst_c" AS "srce_syst_c",
  "t1"."efft_d" AS "efft_d",
  "t1"."expy_d" AS "expy_d",
  "t1"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "%%ddstg%%"."derv_acct_paty_flag" AS "t1"
LEFT JOIN "%%vtech%%"."derv_acct_paty" AS "t2"
  ON "t1"."acct_i" = "t2"."acct_i"
  AND "t1"."paty_acct_rel_c" = "t2"."paty_acct_rel_c"
  AND "t1"."paty_i" = "t2"."paty_i"
  AND "t2"."efft_d" <= :EXTR_D
  AND "t2"."expy_d" >= :EXTR_D
WHERE
  "t1"."efft_d" <= :EXTR_D AND "t1"."expy_d" >= :EXTR_D AND "t2"."acct_i" IS NULL
GROUP BY
  "t1"."acct_i",
  "t1"."paty_i",
  "t1"."assc_acct_i",
  "t1"."paty_acct_rel_c",
  "t1"."prfr_paty_f",
  "t1"."srce_syst_c",
  "t1"."efft_d",
  "t1"."expy_d",
  "t1"."row_secu_accs_c"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_ACCT_PATY, DERV_ACCT_PATY_ADD, DERV_ACCT_PATY_FLAG
- **Columns**: ACCT_I, ASSC_ACCT_I, EFFT_D, EXPY_D, PATY_ACCT_REL_C, PATY_I, PRFR_PATY_F, ROW_SECU_ACCS_C, SRCE_SYST_C
- **Functions**: :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D, T1.ACCT_I = T2.ACCT_I, T1.ACCT_I = T2.ACCT_I AND T1.PATY_I = T2.PATY_I, T1.ACCT_I = T2.ACCT_I AND T1.PATY_I = T2.PATY_I AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C

### SQL Block 3 (Lines 73-73)
- **Complexity Score**: 4
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
DELETE FROM %%DDSTG%%.DERV_ACCT_PATY_CHG;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%DDSTG%%.DERV_ACCT_PATY_CHG
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%ddstg%%"."derv_acct_paty_chg"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_ACCT_PATY_CHG

### SQL Block 4 (Lines 79-103)
- **Complexity Score**: 120
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CHG
SELECT T1.ACCT_I
      ,T1.PATY_I
      ,T1.ASSC_ACCT_I
      ,T1.PATY_ACCT_REL_C
      ,T1.PRFR_PATY_F
      ,T1.SRCE_SYST_C
      ,T1.EFFT_D
      ,T1.EXPY_D
      ,T1.ROW_SECU_ACCS_C
FROM %%DDSTG%%.DERV_ACCT_PATY_FLAG T1
JOIN %%VTECH%%.DERV_ACCT_PATY T2

  ON T1.ACCT_I = T2.ACCT_I
  AND T1.PATY_I = T2.PATY_I
  AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
 
  WHERE (T1.ASSC_ACCT_I <> T2.ASSC_ACCT_I
  OR T1.PRFR_PATY_F <> T2.PRFR_PATY_F
  OR T1.SRCE_SYST_C <> T2.SRCE_SYST_C
  )
  AND :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D
  AND :EXTR_D BETWEEN T2.EFFT_D AND T2.EXPY_D
   GROUP BY 1,2,3,4,5,6,7,8,9;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CHG SELECT T1.ACCT_I, T1.PATY_I, T1.ASSC_ACCT_I, T1.PATY_ACCT_REL_C, T1.PRFR_PATY_F, T1.SRCE_SYST_C, T1.EFFT_D, T1.EXPY_D, T1.ROW_SECU_ACCS_C FROM %%DDSTG%%.DERV_ACCT_PATY_FLAG AS T1 JOIN %%VTECH%%.DERV_ACCT_PATY AS T2 ON T1.ACCT_I = T2.ACCT_I AND T1.PATY_I = T2.PATY_I AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C WHERE (T1.ASSC_ACCT_I <> T2.ASSC_ACCT_I OR T1.PRFR_PATY_F <> T2.PRFR_PATY_F OR T1.SRCE_SYST_C <> T2.SRCE_SYST_C) AND :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D AND :EXTR_D BETWEEN T2.EFFT_D AND T2.EXPY_D GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."derv_acct_paty_chg"
SELECT
  "t1"."acct_i" AS "acct_i",
  "t1"."paty_i" AS "paty_i",
  "t1"."assc_acct_i" AS "assc_acct_i",
  "t1"."paty_acct_rel_c" AS "paty_acct_rel_c",
  "t1"."prfr_paty_f" AS "prfr_paty_f",
  "t1"."srce_syst_c" AS "srce_syst_c",
  "t1"."efft_d" AS "efft_d",
  "t1"."expy_d" AS "expy_d",
  "t1"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "%%ddstg%%"."derv_acct_paty_flag" AS "t1"
JOIN "%%vtech%%"."derv_acct_paty" AS "t2"
  ON "t1"."acct_i" = "t2"."acct_i"
  AND (
    "t1"."assc_acct_i" <> "t2"."assc_acct_i"
    OR "t1"."prfr_paty_f" <> "t2"."prfr_paty_f"
    OR "t1"."srce_syst_c" <> "t2"."srce_syst_c"
  )
  AND "t1"."paty_acct_rel_c" = "t2"."paty_acct_rel_c"
  AND "t1"."paty_i" = "t2"."paty_i"
  AND "t2"."efft_d" <= :EXTR_D
  AND "t2"."expy_d" >= :EXTR_D
WHERE
  "t1"."efft_d" <= :EXTR_D AND "t1"."expy_d" >= :EXTR_D
GROUP BY
  "t1"."acct_i",
  "t1"."paty_i",
  "t1"."assc_acct_i",
  "t1"."paty_acct_rel_c",
  "t1"."prfr_paty_f",
  "t1"."srce_syst_c",
  "t1"."efft_d",
  "t1"."expy_d",
  "t1"."row_secu_accs_c"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_ACCT_PATY, DERV_ACCT_PATY_CHG, DERV_ACCT_PATY_FLAG
- **Columns**: ACCT_I, ASSC_ACCT_I, EFFT_D, EXPY_D, PATY_ACCT_REL_C, PATY_I, PRFR_PATY_F, ROW_SECU_ACCS_C, SRCE_SYST_C
- **Functions**: (T1.ASSC_ACCT_I <> T2.ASSC_ACCT_I OR T1.PRFR_PATY_F <> T2.PRFR_PATY_F OR T1.SRCE_SYST_C <> T2.SRCE_SYST_C), (T1.ASSC_ACCT_I <> T2.ASSC_ACCT_I OR T1.PRFR_PATY_F <> T2.PRFR_PATY_F OR T1.SRCE_SYST_C <> T2.SRCE_SYST_C) AND :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D, T1.ACCT_I = T2.ACCT_I, T1.ACCT_I = T2.ACCT_I AND T1.PATY_I = T2.PATY_I, T1.ASSC_ACCT_I <> T2.ASSC_ACCT_I, T1.ASSC_ACCT_I <> T2.ASSC_ACCT_I OR T1.PRFR_PATY_F <> T2.PRFR_PATY_F

### SQL Block 5 (Lines 112-112)
- **Complexity Score**: 4
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
DELETE FROM %%DDSTG%%.DERV_ACCT_PATY_DEL;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%DDSTG%%.DERV_ACCT_PATY_DEL
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%ddstg%%"."derv_acct_paty_del"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_ACCT_PATY_DEL

### SQL Block 6 (Lines 118-139)
- **Complexity Score**: 101
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_DEL
SELECT T1.ACCT_I
      ,T1.PATY_I
      ,T1.ASSC_ACCT_I
      ,T1.PATY_ACCT_REL_C
      ,T1.PRFR_PATY_F
      ,T1.SRCE_SYST_C
      ,T1.EFFT_D
      ,T1.EXPY_D
      ,T1.ROW_SECU_ACCS_C
FROM %%VTECH%%.DERV_ACCT_PATY T1
LEFT JOIN %%DDSTG%%.DERV_ACCT_PATY_FLAG T2

  ON T1.ACCT_I = T2.ACCT_I
  AND T1.PATY_I = T2.PATY_I
  AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
  AND :EXTR_D BETWEEN T2.EFFT_D AND T2.EXPY_D
 
  WHERE :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D 
    AND T2.ACCT_I IS NULL 
  GROUP BY 1,2,3,4,5,6,7,8,9;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_DEL SELECT T1.ACCT_I, T1.PATY_I, T1.ASSC_ACCT_I, T1.PATY_ACCT_REL_C, T1.PRFR_PATY_F, T1.SRCE_SYST_C, T1.EFFT_D, T1.EXPY_D, T1.ROW_SECU_ACCS_C FROM %%VTECH%%.DERV_ACCT_PATY AS T1 LEFT JOIN %%DDSTG%%.DERV_ACCT_PATY_FLAG AS T2 ON T1.ACCT_I = T2.ACCT_I AND T1.PATY_I = T2.PATY_I AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C AND :EXTR_D BETWEEN T2.EFFT_D AND T2.EXPY_D WHERE :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D AND T2.ACCT_I IS NULL GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."derv_acct_paty_del"
SELECT
  "t1"."acct_i" AS "acct_i",
  "t1"."paty_i" AS "paty_i",
  "t1"."assc_acct_i" AS "assc_acct_i",
  "t1"."paty_acct_rel_c" AS "paty_acct_rel_c",
  "t1"."prfr_paty_f" AS "prfr_paty_f",
  "t1"."srce_syst_c" AS "srce_syst_c",
  "t1"."efft_d" AS "efft_d",
  "t1"."expy_d" AS "expy_d",
  "t1"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "%%vtech%%"."derv_acct_paty" AS "t1"
LEFT JOIN "%%ddstg%%"."derv_acct_paty_flag" AS "t2"
  ON "t1"."acct_i" = "t2"."acct_i"
  AND "t1"."paty_acct_rel_c" = "t2"."paty_acct_rel_c"
  AND "t1"."paty_i" = "t2"."paty_i"
  AND "t2"."efft_d" <= :EXTR_D
  AND "t2"."expy_d" >= :EXTR_D
WHERE
  "t1"."efft_d" <= :EXTR_D AND "t1"."expy_d" >= :EXTR_D AND "t2"."acct_i" IS NULL
GROUP BY
  "t1"."acct_i",
  "t1"."paty_i",
  "t1"."assc_acct_i",
  "t1"."paty_acct_rel_c",
  "t1"."prfr_paty_f",
  "t1"."srce_syst_c",
  "t1"."efft_d",
  "t1"."expy_d",
  "t1"."row_secu_accs_c"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_ACCT_PATY, DERV_ACCT_PATY_DEL, DERV_ACCT_PATY_FLAG
- **Columns**: ACCT_I, ASSC_ACCT_I, EFFT_D, EXPY_D, PATY_ACCT_REL_C, PATY_I, PRFR_PATY_F, ROW_SECU_ACCS_C, SRCE_SYST_C
- **Functions**: :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D, T1.ACCT_I = T2.ACCT_I, T1.ACCT_I = T2.ACCT_I AND T1.PATY_I = T2.PATY_I, T1.ACCT_I = T2.ACCT_I AND T1.PATY_I = T2.PATY_I AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C

## Migration Recommendations

### Suggested Migration Strategy
**High complexity** - Break into multiple models, use DCF full framework

### DCF Mapping
- **Error Handling**: Convert `.IF ERRORCODE` patterns to DCF error handling macros
- **File Operations**: Replace IMPORT/EXPORT with dbt seeds or external tables
- **Statistics**: Replace COLLECT STATS with Snowflake post-hooks
- **Stored Procedures**: Map CALL statements to DCF stored procedure macros

---

*Generated by BTEQ Parser Service - 2025-08-20 12:39:17*
