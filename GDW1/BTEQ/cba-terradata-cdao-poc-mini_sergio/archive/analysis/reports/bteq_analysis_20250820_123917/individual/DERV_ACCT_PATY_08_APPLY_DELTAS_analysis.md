# DERV_ACCT_PATY_08_APPLY_DELTAS.sql - BTEQ Analysis

## File Overview
- **File Name**: DERV_ACCT_PATY_08_APPLY_DELTAS.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 20
- **SQL Blocks**: 8

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 38 | OS_CMD | `.OS rm /cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_PROS_KEY_DATE.txt` |
| 42 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_PROS_KEY.txt` |
| 43 | EXPORT | `.EXPORT DATA FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_PROS_KEY_DATE.tx...` |
| 55 | EXPORT | `.EXPORT RESET` |
| 57 | IF_ERRORCODE | ` .IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 61 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_PROS_KEY_DATE...` |
| 82 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 86 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_PROS_KEY_DATE...` |
| 110 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 115 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_PROS_KEY_DATE...` |
| 153 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 159 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_PROS_KEY_DATE...` |
| 185 | IF_ERRORCODE | `  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 191 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 206 | IF_ERRORCODE | `  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 208 | COLLECT_STATS | ` COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_ROW_SECU_FIX;` |
| 209 | IF_ERRORCODE | `  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 218 | IF_ERRORCODE | `  .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 227 | LABEL | `.LABEL EXITERR` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 48-54)
- **Complexity Score**: 31
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
SELECT  CAST(CAST(PROS_KEY_I AS INTEGER) AS CHAR(10))
         ,CAST(BTCH_RUN_D AS CHAR(10))
        
    FROM %%VTECH%%.UTIL_PROS_ISAC
  WHERE PROS_KEY_I = CAST(trim(:PROSKEY) as DECIMAL(10,0));
```

#### ❄️ Converted Snowflake SQL:
```sql
SELECT CAST(CAST(PROS_KEY_I AS INT) AS CHAR(10)), CAST(BTCH_RUN_D AS CHAR(10)) FROM %%VTECH%%.UTIL_PROS_ISAC WHERE PROS_KEY_I = CAST(TRIM(:PROSKEY) AS DECIMAL(10, 0))
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
SELECT
  CAST(CAST("util_pros_isac"."pros_key_i" AS INT) AS CHAR(10)) AS "pros_key_i",
  CAST("util_pros_isac"."btch_run_d" AS CHAR(10)) AS "btch_run_d"
FROM "%%vtech%%"."util_pros_isac" AS "util_pros_isac"
WHERE
  "util_pros_isac"."pros_key_i" = CAST(TRIM(:PROSKEY) AS DECIMAL(10, 0))
```

#### 📊 SQL Metadata:
- **Tables**: UTIL_PROS_ISAC
- **Columns**: BTCH_RUN_D, PROS_KEY_I
- **Functions**: :PROSKEY, BTCH_RUN_D, CAST(PROS_KEY_I AS INT), PROS_KEY_I, TRIM(:PROSKEY)

### SQL Block 2 (Lines 68-81)
- **Complexity Score**: 0
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ❌ Invalid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
UPDATE T1
FROM   %%STARDATADB%%.DERV_ACCT_PATY T1
       ,%%DDSTG%%.DERV_ACCT_PATY_DEL T2
    SET    EXPY_D = CAST(:EXTR_D AS DATE) -1 (FORMAT'YYYY-MM-DD')(CHAR(10))
          ,PROS_KEY_EXPY_I = CAST(trim(:PROSKEY) as INTEGER)

  WHERE T1.ACCT_I = T2.ACCT_I
    AND T1.PATY_I = T2.PATY_I
    AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
    AND T1.EFFT_D = T2.EFFT_D
    AND :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D
    AND :EXTR_D BETWEEN T2.EFFT_D AND T2.EXPY_D
  ;
```

#### ❄️ Converted Snowflake SQL:
```sql
UPDATE T1
FROM   %%STARDATADB%%.DERV_ACCT_PATY T1
       ,%%DDSTG%%.DERV_ACCT_PATY_DEL T2
    SET    EXPY_D = CAST(:EXTR_D AS DATE) -1 (FORMAT'YYYY-MM-DD')(CHAR(10))
          ,PROS_KEY_EXPY_I = CAST(trim(:PROSKEY) as INTEGER)

  WHERE T1.ACCT_I = T2.ACCT_I
    AND T1.PATY_I = T2.PATY_I
    AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
    AND T1.EFFT_D = T2.EFFT_D
    AND :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D
    AND :EXTR_D BETWEEN T2.EFFT_D AND T2.EXPY_D
  ;
```

#### 🔍 Syntax Validation Details:
- **Valid**: ❌

#### 🎯 Teradata Features Detected:
- Variable substitution

### SQL Block 3 (Lines 93-109)
- **Complexity Score**: 0
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ❌ Invalid
- **Teradata Features**: 3

#### 📝 Original Teradata SQL:
```sql
UPDATE T1
FROM %%STARDATADB%%.DERV_ACCT_PATY T1,
      (sel * from  PDDSTG.DERV_ACCT_PATY_CHG  qualify row_number() over (partition by ACCT_I,PATY_I,PATY_ACCT_REL_C order by efft_d desc)=1)T2

SET EXPY_D = CAST(:EXTR_D AS DATE) -1 (FORMAT'YYYY-MM-DD')(CHAR(10))
    ,PROS_KEY_EXPY_I = CAST(trim(:PROSKEY) as INTEGER)

WHERE T1.ACCT_I = T2.ACCT_I
AND T1.PATY_I = T2.PATY_I
AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
AND (T1.ASSC_ACCT_I <> T2.ASSC_ACCT_I
  OR T1.PRFR_PATY_F <> T2.PRFR_PATY_F
  OR T1.SRCE_SYST_C <> T2.SRCE_SYST_C
  )
AND :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D
AND :EXTR_D BETWEEN T2.EFFT_D AND T2.EXPY_D;
```

#### ❄️ Converted Snowflake SQL:
```sql
UPDATE T1
FROM %%STARDATADB%%.DERV_ACCT_PATY T1,
      (sel * from  PDDSTG.DERV_ACCT_PATY_CHG  qualify row_number() over (partition by ACCT_I,PATY_I,PATY_ACCT_REL_C order by efft_d desc)=1)T2

SET EXPY_D = CAST(:EXTR_D AS DATE) -1 (FORMAT'YYYY-MM-DD')(CHAR(10))
    ,PROS_KEY_EXPY_I = CAST(trim(:PROSKEY) as INTEGER)

WHERE T1.ACCT_I = T2.ACCT_I
AND T1.PATY_I = T2.PATY_I
AND T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
AND (T1.ASSC_ACCT_I <> T2.ASSC_ACCT_I
  OR T1.PRFR_PATY_F <> T2.PRFR_PATY_F
  OR T1.SRCE_SYST_C <> T2.SRCE_SYST_C
  )
AND :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D
AND :EXTR_D BETWEEN T2.EFFT_D AND T2.EXPY_D;
```

#### 🔍 Syntax Validation Details:
- **Valid**: ❌

#### 🎯 Teradata Features Detected:
- QUALIFY clause
- ROW_NUMBER() OVER
- Variable substitution

### SQL Block 4 (Lines 122-152)
- **Complexity Score**: 137
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%STARDATADB%%.DERV_ACCT_PATY  
SEL T1.ACCT_I
      
      ,T1.PATY_I
      ,T1.ASSC_ACCT_I
      ,T1.PATY_ACCT_REL_C
      ,T1.PRFR_PATY_F
      ,T1.SRCE_SYST_C
      ,:EXTR_D AS EFFT_D
      ,T1.EXPY_D
      ,CAST(trim(:PROSKEY) as INTEGER) AS PROS_KEY_EFFT_D
      ,CASE
          WHEN T1.EXPY_D = T1.EFFT_D THEN CAST(trim(:PROSKEY) as INTEGER)
          ELSE 0
       END AS PROS_KEY_EXPY_D
      ,T1.ROW_SECU_ACCS_C
       FROM %%DDSTG%%.DERV_ACCT_PATY_CHG T1
       
       LEFT JOIN %%VTECH%%.DERV_ACCT_PATY T2
       ON T1.ACCT_I = T2.ACCT_I
       AND T1.PATY_I = T2.PATY_I
       AND T1.PATY_aCCT_REL_C = T2.PATY_ACCT_REL_C
       AND T1.ASSC_ACCT_I = T2.ASSC_ACCT_I
       AND T1.SRCE_SYST_C = T2.SRCE_SYST_c
       AND T1.PRFR_PATY_f = T2.PRFR_PATY_f
       AND :EXTR_D BETWEEN t2.EFFT_D AND t2.EXPY_D
  
     WHERE :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D
       AND T2.ACCT_I IS NULL
       ;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%STARDATADB%%.DERV_ACCT_PATY SELECT T1.ACCT_I, T1.PATY_I, T1.ASSC_ACCT_I, T1.PATY_ACCT_REL_C, T1.PRFR_PATY_F, T1.SRCE_SYST_C, :EXTR_D AS EFFT_D, T1.EXPY_D, CAST(TRIM(:PROSKEY) AS INT) AS PROS_KEY_EFFT_D, CASE WHEN T1.EXPY_D = T1.EFFT_D THEN CAST(TRIM(:PROSKEY) AS INT) ELSE 0 END AS PROS_KEY_EXPY_D, T1.ROW_SECU_ACCS_C FROM %%DDSTG%%.DERV_ACCT_PATY_CHG AS T1 LEFT JOIN %%VTECH%%.DERV_ACCT_PATY AS T2 ON T1.ACCT_I = T2.ACCT_I AND T1.PATY_I = T2.PATY_I AND T1.PATY_aCCT_REL_C = T2.PATY_ACCT_REL_C AND T1.ASSC_ACCT_I = T2.ASSC_ACCT_I AND T1.SRCE_SYST_C = T2.SRCE_SYST_c AND T1.PRFR_PATY_f = T2.PRFR_PATY_f AND :EXTR_D BETWEEN t2.EFFT_D AND t2.EXPY_D WHERE :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D AND T2.ACCT_I IS NULL
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%stardatadb%%"."derv_acct_paty"
SELECT
  "t1"."acct_i" AS "acct_i",
  "t1"."paty_i" AS "paty_i",
  "t1"."assc_acct_i" AS "assc_acct_i",
  "t1"."paty_acct_rel_c" AS "paty_acct_rel_c",
  "t1"."prfr_paty_f" AS "prfr_paty_f",
  "t1"."srce_syst_c" AS "srce_syst_c",
  :EXTR_D AS "efft_d",
  "t1"."expy_d" AS "expy_d",
  CAST(TRIM(:PROSKEY) AS INT) AS "pros_key_efft_d",
  CASE WHEN "t1"."efft_d" = "t1"."expy_d" THEN CAST(TRIM(:PROSKEY) AS INT) ELSE 0 END AS "pros_key_expy_d",
  "t1"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "%%ddstg%%"."derv_acct_paty_chg" AS "t1"
LEFT JOIN "%%vtech%%"."derv_acct_paty" AS "t2"
  ON "t1"."acct_i" = "t2"."acct_i"
  AND "t1"."assc_acct_i" = "t2"."assc_acct_i"
  AND "t1"."paty_acct_rel_c" = "t2"."paty_acct_rel_c"
  AND "t1"."paty_i" = "t2"."paty_i"
  AND "t1"."prfr_paty_f" = "t2"."prfr_paty_f"
  AND "t1"."srce_syst_c" = "t2"."srce_syst_c"
  AND "t2"."efft_d" <= :EXTR_D
  AND "t2"."expy_d" >= :EXTR_D
WHERE
  "t1"."efft_d" <= :EXTR_D AND "t1"."expy_d" >= :EXTR_D AND "t2"."acct_i" IS NULL
```

#### 📊 SQL Metadata:
- **Tables**: DERV_ACCT_PATY, DERV_ACCT_PATY_CHG
- **Columns**: ACCT_I, ASSC_ACCT_I, EFFT_D, EXPY_D, PATY_ACCT_REL_C, PATY_I, PATY_aCCT_REL_C, PRFR_PATY_F, PRFR_PATY_f, ROW_SECU_ACCS_C, SRCE_SYST_C, SRCE_SYST_c
- **Functions**: :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D, :PROSKEY, None, T1.ACCT_I = T2.ACCT_I, T1.ACCT_I = T2.ACCT_I AND T1.PATY_I = T2.PATY_I, T1.ACCT_I = T2.ACCT_I AND T1.PATY_I = T2.PATY_I AND T1.PATY_aCCT_REL_C = T2.PATY_ACCT_REL_C, T1.ACCT_I = T2.ACCT_I AND T1.PATY_I = T2.PATY_I AND T1.PATY_aCCT_REL_C = T2.PATY_ACCT_REL_C AND T1.ASSC_ACCT_I = T2.ASSC_ACCT_I, T1.ACCT_I = T2.ACCT_I AND T1.PATY_I = T2.PATY_I AND T1.PATY_aCCT_REL_C = T2.PATY_ACCT_REL_C AND T1.ASSC_ACCT_I = T2.ASSC_ACCT_I AND T1.SRCE_SYST_C = T2.SRCE_SYST_c, T1.ACCT_I = T2.ACCT_I AND T1.PATY_I = T2.PATY_I AND T1.PATY_aCCT_REL_C = T2.PATY_ACCT_REL_C AND T1.ASSC_ACCT_I = T2.ASSC_ACCT_I AND T1.SRCE_SYST_C = T2.SRCE_SYST_c AND T1.PRFR_PATY_f = T2.PRFR_PATY_f, T1.EXPY_D = T1.EFFT_D, TRIM(:PROSKEY)

### SQL Block 5 (Lines 166-184)
- **Complexity Score**: 72
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%STARDATADB%%.DERV_ACCT_PATY
SELECT T1.ACCT_I
      ,T1.PATY_I
      ,T1.ASSC_ACCT_I
      ,T1.PATY_ACCT_REL_C
      ,T1.PRFR_PATY_F
      ,T1.SRCE_SYST_C
      ,T1.EFFT_D
      ,T1.EXPY_D
      ,CAST(trim(:PROSKEY) as INTEGER) AS PROS_KEY_EFFT_D
      ,CASE
          WHEN T1.EXPY_D = T1.EFFT_D THEN CAST(trim(:PROSKEY) as INTEGER)
          ELSE 0
       END AS PROS_KEY_EXPY_D
      ,T1.ROW_SECU_ACCS_C
FROM %%DDSTG%%.DERV_ACCT_PATY_ADD T1

  GROUP BY 1,2,3,4,5,6,7,8,9,10,11;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%STARDATADB%%.DERV_ACCT_PATY SELECT T1.ACCT_I, T1.PATY_I, T1.ASSC_ACCT_I, T1.PATY_ACCT_REL_C, T1.PRFR_PATY_F, T1.SRCE_SYST_C, T1.EFFT_D, T1.EXPY_D, CAST(TRIM(:PROSKEY) AS INT) AS PROS_KEY_EFFT_D, CASE WHEN T1.EXPY_D = T1.EFFT_D THEN CAST(TRIM(:PROSKEY) AS INT) ELSE 0 END AS PROS_KEY_EXPY_D, T1.ROW_SECU_ACCS_C FROM %%DDSTG%%.DERV_ACCT_PATY_ADD AS T1 GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%stardatadb%%"."derv_acct_paty"
SELECT
  "t1"."acct_i" AS "acct_i",
  "t1"."paty_i" AS "paty_i",
  "t1"."assc_acct_i" AS "assc_acct_i",
  "t1"."paty_acct_rel_c" AS "paty_acct_rel_c",
  "t1"."prfr_paty_f" AS "prfr_paty_f",
  "t1"."srce_syst_c" AS "srce_syst_c",
  "t1"."efft_d" AS "efft_d",
  "t1"."expy_d" AS "expy_d",
  CAST(TRIM(:PROSKEY) AS INT) AS "pros_key_efft_d",
  CASE WHEN "t1"."expy_d" = "t1"."efft_d" THEN CAST(TRIM(:PROSKEY) AS INT) ELSE 0 END AS "pros_key_expy_d",
  "t1"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "%%ddstg%%"."derv_acct_paty_add" AS "t1"
GROUP BY
  "t1"."acct_i",
  "t1"."paty_i",
  "t1"."assc_acct_i",
  "t1"."paty_acct_rel_c",
  "t1"."prfr_paty_f",
  "t1"."srce_syst_c",
  "t1"."efft_d",
  "t1"."expy_d",
  CAST(TRIM(:PROSKEY) AS INT),
  CASE WHEN "t1"."expy_d" = "t1"."efft_d" THEN CAST(TRIM(:PROSKEY) AS INT) ELSE 0 END,
  "t1"."row_secu_accs_c"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_ACCT_PATY, DERV_ACCT_PATY_ADD
- **Columns**: ACCT_I, ASSC_ACCT_I, EFFT_D, EXPY_D, PATY_ACCT_REL_C, PATY_I, PRFR_PATY_F, ROW_SECU_ACCS_C, SRCE_SYST_C
- **Functions**: :PROSKEY, None, T1.EXPY_D = T1.EFFT_D, TRIM(:PROSKEY)

### SQL Block 6 (Lines 190-190)
- **Complexity Score**: 6
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
DELETE FROM  %%DDSTG%%.DERV_ACCT_PATY_ROW_SECU_FIX ALL;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%DDSTG%%.DERV_ACCT_PATY_ROW_SECU_FIX AS ALL
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%ddstg%%"."derv_acct_paty_row_secu_fix" AS "all"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_ACCT_PATY_ROW_SECU_FIX

### SQL Block 7 (Lines 194-205)
- **Complexity Score**: 57
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_ROW_SECU_FIX
SEL T1.ACCT_I
, T1.ROW_SECU_ACCS_C AS DERV_ACCT_PATY_ROW_SECU_ACCS_C
, T2.ROW_SECU_ACCS_C AS ACCT_PATY_ROW_SECU_ACCS_C 
FROM %%STARDATADB%%.DERV_ACCT_PATY T1
JOIN %%DDSTG%%.DERV_ACCT_PATY_FLAG T2

ON T1.ACCT_I = T2.ACCT_I
AND T1.PATY_I=  T2.PATY_I
AND T1.ROW_SECU_ACCS_C <>  T2.ROW_SECU_ACCS_C
 
 GROUP BY 1,2,3;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_ROW_SECU_FIX SELECT T1.ACCT_I, T1.ROW_SECU_ACCS_C AS DERV_ACCT_PATY_ROW_SECU_ACCS_C, T2.ROW_SECU_ACCS_C AS ACCT_PATY_ROW_SECU_ACCS_C FROM %%STARDATADB%%.DERV_ACCT_PATY AS T1 JOIN %%DDSTG%%.DERV_ACCT_PATY_FLAG AS T2 ON T1.ACCT_I = T2.ACCT_I AND T1.PATY_I = T2.PATY_I AND T1.ROW_SECU_ACCS_C <> T2.ROW_SECU_ACCS_C GROUP BY 1, 2, 3
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."derv_acct_paty_row_secu_fix"
SELECT
  "t1"."acct_i" AS "acct_i",
  "t1"."row_secu_accs_c" AS "derv_acct_paty_row_secu_accs_c",
  "t2"."row_secu_accs_c" AS "acct_paty_row_secu_accs_c"
FROM "%%stardatadb%%"."derv_acct_paty" AS "t1"
JOIN "%%ddstg%%"."derv_acct_paty_flag" AS "t2"
  ON "t1"."acct_i" = "t2"."acct_i"
  AND "t1"."paty_i" = "t2"."paty_i"
  AND "t1"."row_secu_accs_c" <> "t2"."row_secu_accs_c"
GROUP BY
  "t1"."acct_i",
  "t1"."row_secu_accs_c",
  "t2"."row_secu_accs_c"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_ACCT_PATY, DERV_ACCT_PATY_FLAG, DERV_ACCT_PATY_ROW_SECU_FIX
- **Columns**: ACCT_I, PATY_I, ROW_SECU_ACCS_C
- **Functions**: T1.ACCT_I = T2.ACCT_I, T1.ACCT_I = T2.ACCT_I AND T1.PATY_I = T2.PATY_I

### SQL Block 8 (Lines 211-217)
- **Complexity Score**: 37
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
UPDATE T1
FROM %%STARDATADB%%.DERV_ACCT_PATY T1,
%%DDSTG%%.DERV_ACCT_PATY_ROW_SECU_FIX T2
 
 SET ROW_SECU_ACCS_C = T2.ACCT_PATY_ROW_SECU_ACCS_C
 WHERE T1.ACCT_I = T2.ACCT_I
 AND T1.ROW_SECU_ACCS_C = T2.DERV_ACCT_PATY_ROW_SECU_ACCS_C;
```

#### ❄️ Converted Snowflake SQL:
```sql
UPDATE T1 SET ROW_SECU_ACCS_C = T2.ACCT_PATY_ROW_SECU_ACCS_C FROM %%STARDATADB%%.DERV_ACCT_PATY AS T1, %%DDSTG%%.DERV_ACCT_PATY_ROW_SECU_FIX AS T2 WHERE T1.ACCT_I = T2.ACCT_I AND T1.ROW_SECU_ACCS_C = T2.DERV_ACCT_PATY_ROW_SECU_ACCS_C
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
UPDATE "t1" SET "row_secu_accs_c" = "t2"."acct_paty_row_secu_accs_c"
FROM "%%stardatadb%%"."derv_acct_paty" AS "t1"
  CROSS JOIN "%%ddstg%%"."derv_acct_paty_row_secu_fix" AS "t2"
WHERE
  "t1"."acct_i" = "t2"."acct_i"
  AND "t1"."row_secu_accs_c" = "t2"."derv_acct_paty_row_secu_accs_c"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_ACCT_PATY, DERV_ACCT_PATY_ROW_SECU_FIX, T1
- **Columns**: ACCT_I, ACCT_PATY_ROW_SECU_ACCS_C, DERV_ACCT_PATY_ROW_SECU_ACCS_C, ROW_SECU_ACCS_C
- **Functions**: T1.ACCT_I = T2.ACCT_I

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
