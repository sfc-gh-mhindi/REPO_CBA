# ACCT_BALN_BKDT_AUDT_ISRT.sql - BTEQ Analysis

## File Overview
- **File Name**: ACCT_BALN_BKDT_AUDT_ISRT.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 7
- **SQL Blocks**: 2

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 2 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 90 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 105 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 108 | LOGOFF | `.LOGOFF` |
| 111 | LABEL | `.LABEL EXITERR` |
| 113 | LOGOFF | `.LOGOFF` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 31-89)
- **Complexity Score**: 142
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO  %%CAD_PROD_DATA%%.ACCT_BALN_BKDT_AUDT
(
ACCT_I,                        
BALN_TYPE_C,                   
CALC_FUNC_C,                   
TIME_PERD_C,                   
BALN_A,                        
CALC_F,                        
SRCE_SYST_C,                   
ORIG_SRCE_SYST_C,              
LOAD_D,                        
BKDT_EFFT_D,                   
BKDT_EXPY_D,                   
PROS_KEY_EFFT_I,               
ABAL_PROS_KEY_EFFT_I,          
ABAL_PROS_KEY_EXPY_I,          
ABAL_BKDT_PROS_KEY_I,     
ADJ_PROS_KEY_EFFT_I
)
SELECT 
STG1.ACCT_I,                        
STG1.BALN_TYPE_C,
STG1.CALC_FUNC_C,                  
STG1.TIME_PERD_C,                  
STG1.BALN_A,
STG1.CALC_F,
STG1.SRCE_SYST_C,
STG1.ORIG_SRCE_SYST_C,
STG1.LOAD_D,
STG1.BKDT_EFFT_D,
STG1.BKDT_EXPY_D,
PKEY.PROS_KEY_EFFT_I,
STG1.PROS_KEY_EFFT_I AS ABAL_PROS_KEY_EFFT_I,
STG1.PROS_KEY_EXPY_I AS ABAL_PROS_KEY_EXPY_I,
STG2.BKDT_PROS_KEY_I AS ABAL_BKDT_PROS_KEY_I,
ADJ.PROS_KEY_EFFT_I AS ADJ_PROS_KEY_EFFT_I
FROM 
%%DDSTG%%.ACCT_BALN_BKDT_STG1 STG1
INNER JOIN
/*Capturing the maximum pros_key_efft_i in the event of multiple pros keys for one account 
and populating for Auditing purposes*/
(SELECT ACCT_I, MAX(PROS_KEY_EFFT_I) AS PROS_KEY_EFFT_I FROM
%%DDSTG%%.ACCT_BALN_BKDT_ADJ_RULE
GROUP BY 1
)ADJ
ON
STG1.ACCT_I = ADJ.ACCT_I
CROSS JOIN
/*Capture tha latest pros key [from the parent process] and update the audt table*/
(SELECT MAX(BKDT_PROS_KEY_I) AS BKDT_PROS_KEY_I
FROM %%DDSTG%%.ACCT_BALN_BKDT_STG2)STG2
CROSS JOIN
/*Capture tha latest pros key [from the Auditing process] and update the audt table*/
(SELECT MAX(PROS_KEY_I)  AS PROS_KEY_EFFT_I
FROM %%VTECH%%.UTIL_PROS_ISAC WHERE 
CONV_M='CAD_X01_ACCT_BALN_BKDT_AUDT')PKEY
 ;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%CAD_PROD_DATA%%.ACCT_BALN_BKDT_AUDT (ACCT_I, BALN_TYPE_C, CALC_FUNC_C, TIME_PERD_C, BALN_A, CALC_F, SRCE_SYST_C, ORIG_SRCE_SYST_C, LOAD_D, BKDT_EFFT_D, BKDT_EXPY_D, PROS_KEY_EFFT_I, ABAL_PROS_KEY_EFFT_I, ABAL_PROS_KEY_EXPY_I, ABAL_BKDT_PROS_KEY_I, ADJ_PROS_KEY_EFFT_I) SELECT STG1.ACCT_I, STG1.BALN_TYPE_C, STG1.CALC_FUNC_C, STG1.TIME_PERD_C, STG1.BALN_A, STG1.CALC_F, STG1.SRCE_SYST_C, STG1.ORIG_SRCE_SYST_C, STG1.LOAD_D, STG1.BKDT_EFFT_D, STG1.BKDT_EXPY_D, PKEY.PROS_KEY_EFFT_I, STG1.PROS_KEY_EFFT_I AS ABAL_PROS_KEY_EFFT_I, STG1.PROS_KEY_EXPY_I AS ABAL_PROS_KEY_EXPY_I, STG2.BKDT_PROS_KEY_I AS ABAL_BKDT_PROS_KEY_I, ADJ.PROS_KEY_EFFT_I AS ADJ_PROS_KEY_EFFT_I FROM %%DDSTG%%.ACCT_BALN_BKDT_STG1 AS STG1 INNER JOIN (SELECT ACCT_I, MAX(PROS_KEY_EFFT_I) AS PROS_KEY_EFFT_I FROM %%DDSTG%%.ACCT_BALN_BKDT_ADJ_RULE GROUP BY 1) AS ADJ ON STG1.ACCT_I = ADJ.ACCT_I CROSS JOIN (SELECT MAX(BKDT_PROS_KEY_I) AS BKDT_PROS_KEY_I FROM %%DDSTG%%.ACCT_BALN_BKDT_STG2) AS STG2 CROSS JOIN (SELECT MAX(PROS_KEY_I) AS PROS_KEY_EFFT_I FROM %%VTECH%%.UTIL_PROS_ISAC WHERE CONV_M = 'CAD_X01_ACCT_BALN_BKDT_AUDT') AS PKEY
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%cad_prod_data%%"."acct_baln_bkdt_audt" (
  "acct_i",
  "baln_type_c",
  "calc_func_c",
  "time_perd_c",
  "baln_a",
  "calc_f",
  "srce_syst_c",
  "orig_srce_syst_c",
  "load_d",
  "bkdt_efft_d",
  "bkdt_expy_d",
  "pros_key_efft_i",
  "abal_pros_key_efft_i",
  "abal_pros_key_expy_i",
  "abal_bkdt_pros_key_i",
  "adj_pros_key_efft_i"
)
WITH "pkey" AS (
  SELECT
    MAX("util_pros_isac"."pros_key_i") AS "pros_key_efft_i"
  FROM "%%vtech%%"."util_pros_isac" AS "util_pros_isac"
  WHERE
    "util_pros_isac"."conv_m" = 'CAD_X01_ACCT_BALN_BKDT_AUDT'
), "stg2" AS (
  SELECT
    MAX("acct_baln_bkdt_stg2"."bkdt_pros_key_i") AS "bkdt_pros_key_i"
  FROM "%%ddstg%%"."acct_baln_bkdt_stg2" AS "acct_baln_bkdt_stg2"
), "adj" AS (
  SELECT
    "acct_baln_bkdt_adj_rule"."acct_i" AS "acct_i",
    MAX("acct_baln_bkdt_adj_rule"."pros_key_efft_i") AS "pros_key_efft_i"
  FROM "%%ddstg%%"."acct_baln_bkdt_adj_rule" AS "acct_baln_bkdt_adj_rule"
  GROUP BY
    "acct_baln_bkdt_adj_rule"."acct_i"
)
SELECT
  "stg1"."acct_i" AS "acct_i",
  "stg1"."baln_type_c" AS "baln_type_c",
  "stg1"."calc_func_c" AS "calc_func_c",
  "stg1"."time_perd_c" AS "time_perd_c",
  "stg1"."baln_a" AS "baln_a",
  "stg1"."calc_f" AS "calc_f",
  "stg1"."srce_syst_c" AS "srce_syst_c",
  "stg1"."orig_srce_syst_c" AS "orig_srce_syst_c",
  "stg1"."load_d" AS "load_d",
  "stg1"."bkdt_efft_d" AS "bkdt_efft_d",
  "stg1"."bkdt_expy_d" AS "bkdt_expy_d",
  "pkey"."pros_key_efft_i" AS "pros_key_efft_i",
  "stg1"."pros_key_efft_i" AS "abal_pros_key_efft_i",
  "stg1"."pros_key_expy_i" AS "abal_pros_key_expy_i",
  "stg2"."bkdt_pros_key_i" AS "abal_bkdt_pros_key_i",
  "adj"."pros_key_efft_i" AS "adj_pros_key_efft_i"
FROM "%%ddstg%%"."acct_baln_bkdt_stg1" AS "stg1"
CROSS JOIN "pkey" AS "pkey"
CROSS JOIN "stg2" AS "stg2"
JOIN "adj" AS "adj"
  ON "adj"."acct_i" = "stg1"."acct_i"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_BALN_BKDT_ADJ_RULE, ACCT_BALN_BKDT_AUDT, ACCT_BALN_BKDT_STG1, ACCT_BALN_BKDT_STG2, UTIL_PROS_ISAC
- **Columns**: ACCT_I, BALN_A, BALN_TYPE_C, BKDT_EFFT_D, BKDT_EXPY_D, BKDT_PROS_KEY_I, CALC_F, CALC_FUNC_C, CONV_M, LOAD_D, ORIG_SRCE_SYST_C, PROS_KEY_EFFT_I, PROS_KEY_EXPY_I, PROS_KEY_I, SRCE_SYST_C, TIME_PERD_C
- **Functions**: BKDT_PROS_KEY_I, PROS_KEY_EFFT_I, PROS_KEY_I

### SQL Block 2 (Lines 93-104)
- **Complexity Score**: 53
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
UPDATE %%CAD_PROD_DATA%%.UTIL_PROS_ISAC
FROM
(SELECT COUNT(*) FROM 
%%DDSTG%%.ACCT_BALN_BKDT_STG1)A(INS_CNT)
SET  
        COMT_F = 'Y',
	SUCC_F='Y',
	COMT_S =  CURRENT_TIMESTAMP(0),
	SYST_INS_Q = A.INS_CNT
 WHERE PROS_KEY_I = (SELECT MAX(PROS_KEY_I) FROM %%VTECH%%.UTIL_PROS_ISAC 
 WHERE CONV_M='CAD_X01_ACCT_BALN_BKDT_AUDT');
```

#### ❄️ Converted Snowflake SQL:
```sql
UPDATE %%CAD_PROD_DATA%%.UTIL_PROS_ISAC SET COMT_F = 'Y', SUCC_F = 'Y', COMT_S = CURRENT_TIMESTAMP(0), SYST_INS_Q = A.INS_CNT FROM (SELECT COUNT(*) FROM %%DDSTG%%.ACCT_BALN_BKDT_STG1) AS A(INS_CNT) WHERE PROS_KEY_I = (SELECT MAX(PROS_KEY_I) FROM %%VTECH%%.UTIL_PROS_ISAC WHERE CONV_M = 'CAD_X01_ACCT_BALN_BKDT_AUDT')
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
UPDATE "%%cad_prod_data%%"."util_pros_isac" SET "comt_f" = 'Y', "succ_f" = 'Y', "comt_s" = CURRENT_TIMESTAMP(0), "a"."ins_cnt" = "syst_ins_q"
FROM (
  SELECT
    COUNT(*) AS "_col_0"
  FROM "%%ddstg%%"."acct_baln_bkdt_stg1" AS "acct_baln_bkdt_stg1"
) AS "a"("ins_cnt")
WHERE
  "pros_key_i" = (
    SELECT
      MAX("util_pros_isac"."pros_key_i") AS "_col_0"
    FROM "%%vtech%%"."util_pros_isac" AS "util_pros_isac"
    WHERE
      "util_pros_isac"."conv_m" = 'CAD_X01_ACCT_BALN_BKDT_AUDT'
  )
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_BALN_BKDT_STG1, UTIL_PROS_ISAC
- **Columns**: COMT_F, COMT_S, CONV_M, INS_CNT, PROS_KEY_I, SUCC_F, SYST_INS_Q
- **Functions**: *, 0, PROS_KEY_I

## Migration Recommendations

### Suggested Migration Strategy
**Medium complexity** - Incremental model with DCF hooks recommended

### DCF Mapping
- **Error Handling**: Convert `.IF ERRORCODE` patterns to DCF error handling macros
- **File Operations**: Replace IMPORT/EXPORT with dbt seeds or external tables
- **Statistics**: Replace COLLECT STATS with Snowflake post-hooks
- **Stored Procedures**: Map CALL statements to DCF stored procedure macros

---

*Generated by BTEQ Parser Service - 2025-08-20 12:39:23*
