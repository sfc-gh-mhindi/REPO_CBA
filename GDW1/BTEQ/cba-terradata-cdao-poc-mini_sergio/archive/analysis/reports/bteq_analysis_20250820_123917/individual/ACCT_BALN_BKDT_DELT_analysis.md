# ACCT_BALN_BKDT_DELT.sql - BTEQ Analysis

## File Overview
- **File Name**: ACCT_BALN_BKDT_DELT.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 6
- **SQL Blocks**: 1

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 2 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 41 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 44 | LOGOFF | `.LOGOFF` |
| 47 | LABEL | `.LABEL EXITERR` |
| 49 | LOGOFF | `.LOGOFF` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 23-40)
- **Complexity Score**: 102
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
DELETE BAL
/* Deleting the records from the ACCT_BALN_BKDT table. These records are modified 
as a result of applying adjustment and so will be resinserted from STG2 at next step*/
FROM
 %%CAD_PROD_DATA%%.ACCT_BALN_BKDT BAL,
 %%DDSTG%%.ACCT_BALN_BKDT_STG1 STG1
 WHERE 
STG1.ACCT_I = BAL.ACCT_I    
AND STG1.BALN_TYPE_C = BAL.BALN_TYPE_C                    
AND STG1.CALC_FUNC_C = BAL.CALC_FUNC_C                   
AND STG1.TIME_PERD_C = BAL.TIME_PERD_C                   
AND STG1.BKDT_EFFT_D = BAL.BKDT_EFFT_D                        
AND STG1.BKDT_EXPY_D = BAL.BKDT_EXPY_D                        
AND STG1.BALN_A = BAL.BALN_A                        
AND STG1.CALC_F = BAL.CALC_F                        
AND COALESCE(STG1.PROS_KEY_EFFT_I,0) = COALESCE(BAL.PROS_KEY_EFFT_I,0)
AND COALESCE(STG1.PROS_KEY_EXPY_I,0) = COALESCE(BAL.PROS_KEY_EXPY_I,0);
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE BAL FROM %%CAD_PROD_DATA%%.ACCT_BALN_BKDT AS BAL, %%DDSTG%%.ACCT_BALN_BKDT_STG1 AS STG1 WHERE STG1.ACCT_I = BAL.ACCT_I AND STG1.BALN_TYPE_C = BAL.BALN_TYPE_C AND STG1.CALC_FUNC_C = BAL.CALC_FUNC_C AND STG1.TIME_PERD_C = BAL.TIME_PERD_C AND STG1.BKDT_EFFT_D = BAL.BKDT_EFFT_D AND STG1.BKDT_EXPY_D = BAL.BKDT_EXPY_D AND STG1.BALN_A = BAL.BALN_A AND STG1.CALC_F = BAL.CALC_F AND COALESCE(STG1.PROS_KEY_EFFT_I, 0) = COALESCE(BAL.PROS_KEY_EFFT_I, 0) AND COALESCE(STG1.PROS_KEY_EXPY_I, 0) = COALESCE(BAL.PROS_KEY_EXPY_I, 0)
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE   "bal" FROM "%%cad_prod_data%%"."acct_baln_bkdt" AS "bal"
  CROSS JOIN "%%ddstg%%"."acct_baln_bkdt_stg1" AS "stg1"
WHERE
  "bal"."acct_i" = "stg1"."acct_i"
  AND "bal"."baln_a" = "stg1"."baln_a"
  AND "bal"."baln_type_c" = "stg1"."baln_type_c"
  AND "bal"."bkdt_efft_d" = "stg1"."bkdt_efft_d"
  AND "bal"."bkdt_expy_d" = "stg1"."bkdt_expy_d"
  AND "bal"."calc_f" = "stg1"."calc_f"
  AND "bal"."calc_func_c" = "stg1"."calc_func_c"
  AND "bal"."time_perd_c" = "stg1"."time_perd_c"
  AND COALESCE("bal"."pros_key_efft_i", 0) = COALESCE("stg1"."pros_key_efft_i", 0)
  AND COALESCE("bal"."pros_key_expy_i", 0) = COALESCE("stg1"."pros_key_expy_i", 0)
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_BALN_BKDT, ACCT_BALN_BKDT_STG1, BAL
- **Columns**: ACCT_I, BALN_A, BALN_TYPE_C, BKDT_EFFT_D, BKDT_EXPY_D, CALC_F, CALC_FUNC_C, PROS_KEY_EFFT_I, PROS_KEY_EXPY_I, TIME_PERD_C
- **Functions**: BAL.PROS_KEY_EFFT_I, BAL.PROS_KEY_EXPY_I, STG1.ACCT_I = BAL.ACCT_I, STG1.ACCT_I = BAL.ACCT_I AND STG1.BALN_TYPE_C = BAL.BALN_TYPE_C, STG1.ACCT_I = BAL.ACCT_I AND STG1.BALN_TYPE_C = BAL.BALN_TYPE_C AND STG1.CALC_FUNC_C = BAL.CALC_FUNC_C, STG1.ACCT_I = BAL.ACCT_I AND STG1.BALN_TYPE_C = BAL.BALN_TYPE_C AND STG1.CALC_FUNC_C = BAL.CALC_FUNC_C AND STG1.TIME_PERD_C = BAL.TIME_PERD_C, STG1.ACCT_I = BAL.ACCT_I AND STG1.BALN_TYPE_C = BAL.BALN_TYPE_C AND STG1.CALC_FUNC_C = BAL.CALC_FUNC_C AND STG1.TIME_PERD_C = BAL.TIME_PERD_C AND STG1.BKDT_EFFT_D = BAL.BKDT_EFFT_D, STG1.ACCT_I = BAL.ACCT_I AND STG1.BALN_TYPE_C = BAL.BALN_TYPE_C AND STG1.CALC_FUNC_C = BAL.CALC_FUNC_C AND STG1.TIME_PERD_C = BAL.TIME_PERD_C AND STG1.BKDT_EFFT_D = BAL.BKDT_EFFT_D AND STG1.BKDT_EXPY_D = BAL.BKDT_EXPY_D, STG1.ACCT_I = BAL.ACCT_I AND STG1.BALN_TYPE_C = BAL.BALN_TYPE_C AND STG1.CALC_FUNC_C = BAL.CALC_FUNC_C AND STG1.TIME_PERD_C = BAL.TIME_PERD_C AND STG1.BKDT_EFFT_D = BAL.BKDT_EFFT_D AND STG1.BKDT_EXPY_D = BAL.BKDT_EXPY_D AND STG1.BALN_A = BAL.BALN_A, STG1.ACCT_I = BAL.ACCT_I AND STG1.BALN_TYPE_C = BAL.BALN_TYPE_C AND STG1.CALC_FUNC_C = BAL.CALC_FUNC_C AND STG1.TIME_PERD_C = BAL.TIME_PERD_C AND STG1.BKDT_EFFT_D = BAL.BKDT_EFFT_D AND STG1.BKDT_EXPY_D = BAL.BKDT_EXPY_D AND STG1.BALN_A = BAL.BALN_A AND STG1.CALC_F = BAL.CALC_F, STG1.ACCT_I = BAL.ACCT_I AND STG1.BALN_TYPE_C = BAL.BALN_TYPE_C AND STG1.CALC_FUNC_C = BAL.CALC_FUNC_C AND STG1.TIME_PERD_C = BAL.TIME_PERD_C AND STG1.BKDT_EFFT_D = BAL.BKDT_EFFT_D AND STG1.BKDT_EXPY_D = BAL.BKDT_EXPY_D AND STG1.BALN_A = BAL.BALN_A AND STG1.CALC_F = BAL.CALC_F AND COALESCE(STG1.PROS_KEY_EFFT_I, 0) = COALESCE(BAL.PROS_KEY_EFFT_I, 0), STG1.PROS_KEY_EFFT_I, STG1.PROS_KEY_EXPY_I

## Migration Recommendations

### Suggested Migration Strategy
**Medium complexity** - Incremental model with DCF hooks recommended

### DCF Mapping
- **Error Handling**: Convert `.IF ERRORCODE` patterns to DCF error handling macros
- **File Operations**: Replace IMPORT/EXPORT with dbt seeds or external tables
- **Statistics**: Replace COLLECT STATS with Snowflake post-hooks
- **Stored Procedures**: Map CALL statements to DCF stored procedure macros

---

*Generated by BTEQ Parser Service - 2025-08-20 12:39:18*
