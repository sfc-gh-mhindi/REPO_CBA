# ACCT_BALN_BKDT_RECN_ISRT.sql - BTEQ Analysis

## File Overview
- **File Name**: ACCT_BALN_BKDT_RECN_ISRT.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 11
- **SQL Blocks**: 5

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 2 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 25 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 111 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 199 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 213 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 216 | LOGOFF | `.LOGOFF` |
| 219 | LABEL | `.LABEL EXITERR` |
| 221 | LOGOFF | `.LOGOFF` |
| 225 | LABEL | `.LABEL ERR_SEV` |
| 236 | LOGOFF | `.LOGOFF` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 23-24)
- **Complexity Score**: 4
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
DELETE %%CAD_PROD_DATA%%.ACCT_BALN_BKDT_RECN;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE %%CAD_PROD_DATA%%.ACCT_BALN_BKDT_RECN
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE   "%%cad_prod_data%%"."acct_baln_bkdt_recn"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_BALN_BKDT_RECN

### SQL Block 2 (Lines 28-110)
- **Complexity Score**: 0
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ❌ Invalid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%CAD_PROD_DATA%%.ACCT_BALN_BKDT_RECN
SELECT 
DT.ACCT_I
,BAL.EFFT_D
,BAL.EXPY_D
,DT.BALN_A
,NULL AS PROS_KEY_EFFT_I
FROM
(
SELECT 
B.ACCT_I,
B.BALN_A
FROM
/*Qualifying only those records that are considered for applying adjustments as part of this run*/
%%DDSTG%%.ACCT_BALN_BKDT_STG2 A
INNER JOIN
%%VTECH%%.ACCT_BALN B
ON
A.ACCT_I=B.ACCT_I
WHERE
B.BALN_TYPE_C='BALN'
AND B.CALC_FUNC_C='SPOT' 
AND B.TIME_PERD_C = 'E' 
/*reconciling the balances only for the current record*/
AND CURRENT_DATE BETWEEN B.EFFT_D AND B.EXPY_D

MINUS

SELECT 
STG.ACCT_I,
(CASE WHEN STG.BKDT_EFFT_D > BKDT.BKDT_EFFT_D  THEN STG.BALN_A 
ELSE BKDT.BALN_A END ) AS BALN_A
FROM
(
SELECT 
A.ACCT_I
,A.BKDT_EFFT_D
,A.BKDT_EXPY_D
,A.BALN_A
FROM
%%DDSTG%%.ACCT_BALN_BKDT_STG2 A
WHERE
A.BALN_TYPE_C='BDCL'
AND A.CALC_FUNC_C='SPOT' 
AND A.TIME_PERD_C = 'E' 
AND CURRENT_DATE BETWEEN A.BKDT_EFFT_D AND A.BKDT_EXPY_D
)STG
INNER JOIN
(
SELECT 
B.ACCT_I
,B.BKDT_EFFT_D
,B.BKDT_EXPY_D
,B.BALN_A
FROM
%%DDSTG%%.ACCT_BALN_BKDT_STG2 A
INNER JOIN
%%VTECH%%.ACCT_BALN_BKDT B
ON
A.ACCT_I = B.ACCT_I
WHERE
B.BALN_TYPE_C='BDCL'
AND B.CALC_FUNC_C='SPOT' 
AND B.TIME_PERD_C = 'E' 
AND CURRENT_DATE BETWEEN B.BKDT_EFFT_D AND B.BKDT_EXPY_D
)BKDT
ON
STG.ACCT_I = BKDT.ACCT_I
)DT
INNER JOIN
%%VTECH%%.ACCT_BALN BAL
ON
DT.ACCT_I = BAL.ACCT_I
WHERE
BAL.BALN_TYPE_C='BDCL'
AND BAL.CALC_FUNC_C='SPOT' 
AND BAL.TIME_PERD_C = 'E' 
AND CURRENT_DATE BETWEEN BAL.EFFT_D AND BAL.EXPY_D
;

.IF ACTIVITYCOUNT<>0 THEN .GOTO ERR_SEV
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%CAD_PROD_DATA%%.ACCT_BALN_BKDT_RECN
SELECT 
DT.ACCT_I
,BAL.EFFT_D
,BAL.EXPY_D
,DT.BALN_A
,NULL AS PROS_KEY_EFFT_I
FROM
(
SELECT 
B.ACCT_I,
B.BALN_A
FROM
/*Qualifying only those records that are considered for applying adjustments as part of this run*/
%%DDSTG%%.ACCT_BALN_BKDT_STG2 A
INNER JOIN
%%VTECH%%.ACCT_BALN B
ON
A.ACCT_I=B.ACCT_I
WHERE
B.BALN_TYPE_C='BALN'
AND B.CALC_FUNC_C='SPOT' 
AND B.TIME_PERD_C = 'E' 
/*reconciling the balances only for the current record*/
AND CURRENT_DATE BETWEEN B.EFFT_D AND B.EXPY_D

MINUS

SELECT 
STG.ACCT_I,
(CASE WHEN STG.BKDT_EFFT_D > BKDT.BKDT_EFFT_D  THEN STG.BALN_A 
ELSE BKDT.BALN_A END ) AS BALN_A
FROM
(
SELECT 
A.ACCT_I
,A.BKDT_EFFT_D
,A.BKDT_EXPY_D
,A.BALN_A
FROM
%%DDSTG%%.ACCT_BALN_BKDT_STG2 A
WHERE
A.BALN_TYPE_C='BDCL'
AND A.CALC_FUNC_C='SPOT' 
AND A.TIME_PERD_C = 'E' 
AND CURRENT_DATE BETWEEN A.BKDT_EFFT_D AND A.BKDT_EXPY_D
)STG
INNER JOIN
(
SELECT 
B.ACCT_I
,B.BKDT_EFFT_D
,B.BKDT_EXPY_D
,B.BALN_A
FROM
%%DDSTG%%.ACCT_BALN_BKDT_STG2 A
INNER JOIN
%%VTECH%%.ACCT_BALN_BKDT B
ON
A.ACCT_I = B.ACCT_I
WHERE
B.BALN_TYPE_C='BDCL'
AND B.CALC_FUNC_C='SPOT' 
AND B.TIME_PERD_C = 'E' 
AND CURRENT_DATE BETWEEN B.BKDT_EFFT_D AND B.BKDT_EXPY_D
)BKDT
ON
STG.ACCT_I = BKDT.ACCT_I
)DT
INNER JOIN
%%VTECH%%.ACCT_BALN BAL
ON
DT.ACCT_I = BAL.ACCT_I
WHERE
BAL.BALN_TYPE_C='BDCL'
AND BAL.CALC_FUNC_C='SPOT' 
AND BAL.TIME_PERD_C = 'E' 
AND CURRENT_DATE BETWEEN BAL.EFFT_D AND BAL.EXPY_D
;

.IF ACTIVITYCOUNT<>0 THEN .GOTO ERR_SEV
```

#### 🔍 Syntax Validation Details:
- **Valid**: ❌

#### 🎯 Teradata Features Detected:
- Variable substitution

### SQL Block 3 (Lines 114-198)
- **Complexity Score**: 0
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ❌ Invalid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%CAD_PROD_DATA%%.ACCT_BALN_BKDT_RECN
SELECT 
 DT1.ACCT_I
,STG.BKDT_EFFT_D
,STG.BKDT_EXPY_D
,STG.BALN_A
,NULL AS PROS_KEY_EFFT_I
FROM
(SELECT 
STG.ACCT_I,
(CASE WHEN STG.BKDT_EFFT_D > BKDT.BKDT_EFFT_D  THEN STG.BALN_A 
ELSE BKDT.BALN_A END ) AS BALN_A,
NULL AS PROS_KEY_EFFT_I
FROM
(
SELECT 
A.ACCT_I
,A.BKDT_EFFT_D
,A.BKDT_EXPY_D
,A.BALN_A
FROM
%%DDSTG%%.ACCT_BALN_BKDT_STG2 A
WHERE
A.BALN_TYPE_C='BDCL'
AND A.CALC_FUNC_C='SPOT' 
AND A.TIME_PERD_C = 'E' 
AND CURRENT_DATE BETWEEN A.BKDT_EFFT_D AND A.BKDT_EXPY_D
)STG
INNER JOIN
(
SELECT 
B.ACCT_I
,B.BKDT_EFFT_D
,B.BKDT_EXPY_D
,B.BALN_A
FROM
%%DDSTG%%.ACCT_BALN_BKDT_STG2 A
INNER JOIN
%%VTECH%%.ACCT_BALN_BKDT B
ON
A.ACCT_I = B.ACCT_I
WHERE
B.BALN_TYPE_C='BDCL'
AND B.CALC_FUNC_C='SPOT' 
AND B.TIME_PERD_C = 'E' 
AND CURRENT_DATE BETWEEN B.BKDT_EFFT_D AND B.BKDT_EXPY_D
)BKDT
ON
STG.ACCT_I = BKDT.ACCT_I

MINUS

SELECT 
B.ACCT_I,
B.BALN_A,
NULL AS PROS_KEY_EFFT_I
FROM
/*Qualifying only those records that are considered for applying adjustments as part of this run*/
%%DDSTG%%.ACCT_BALN_BKDT_STG2 A
INNER JOIN
%%VTECH%%.ACCT_BALN B
ON
A.ACCT_I=B.ACCT_I
WHERE
B.BALN_TYPE_C='BALN'
AND B.CALC_FUNC_C='SPOT' 
AND B.TIME_PERD_C = 'E' 
/*reconciling the balances only for the current record*/
AND CURRENT_DATE BETWEEN B.EFFT_D AND B.EXPY_D
)DT1
INNER JOIN
%%DDSTG%%.ACCT_BALN_BKDT_STG2 STG
ON
DT1.ACCT_I=STG.ACCT_I
AND CURRENT_DATE BETWEEN STG.BKDT_EFFT_D AND STG.BKDT_EXPY_D
;


/*Activity Count > 0 implies - An adjustment affecting the balances of an open record for 
the ACCT_I loaded into ACCT_BALN_BKDT_RECN table.This impacts the next daily delta 
load of Datastage. Thereby it is highly recommended to fix the issue 
before you restart in such a failures*/

.IF ACTIVITYCOUNT<>0 THEN .GOTO ERR_SEV
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%CAD_PROD_DATA%%.ACCT_BALN_BKDT_RECN
SELECT 
 DT1.ACCT_I
,STG.BKDT_EFFT_D
,STG.BKDT_EXPY_D
,STG.BALN_A
,NULL AS PROS_KEY_EFFT_I
FROM
(SELECT 
STG.ACCT_I,
(CASE WHEN STG.BKDT_EFFT_D > BKDT.BKDT_EFFT_D  THEN STG.BALN_A 
ELSE BKDT.BALN_A END ) AS BALN_A,
NULL AS PROS_KEY_EFFT_I
FROM
(
SELECT 
A.ACCT_I
,A.BKDT_EFFT_D
,A.BKDT_EXPY_D
,A.BALN_A
FROM
%%DDSTG%%.ACCT_BALN_BKDT_STG2 A
WHERE
A.BALN_TYPE_C='BDCL'
AND A.CALC_FUNC_C='SPOT' 
AND A.TIME_PERD_C = 'E' 
AND CURRENT_DATE BETWEEN A.BKDT_EFFT_D AND A.BKDT_EXPY_D
)STG
INNER JOIN
(
SELECT 
B.ACCT_I
,B.BKDT_EFFT_D
,B.BKDT_EXPY_D
,B.BALN_A
FROM
%%DDSTG%%.ACCT_BALN_BKDT_STG2 A
INNER JOIN
%%VTECH%%.ACCT_BALN_BKDT B
ON
A.ACCT_I = B.ACCT_I
WHERE
B.BALN_TYPE_C='BDCL'
AND B.CALC_FUNC_C='SPOT' 
AND B.TIME_PERD_C = 'E' 
AND CURRENT_DATE BETWEEN B.BKDT_EFFT_D AND B.BKDT_EXPY_D
)BKDT
ON
STG.ACCT_I = BKDT.ACCT_I

MINUS

SELECT 
B.ACCT_I,
B.BALN_A,
NULL AS PROS_KEY_EFFT_I
FROM
/*Qualifying only those records that are considered for applying adjustments as part of this run*/
%%DDSTG%%.ACCT_BALN_BKDT_STG2 A
INNER JOIN
%%VTECH%%.ACCT_BALN B
ON
A.ACCT_I=B.ACCT_I
WHERE
B.BALN_TYPE_C='BALN'
AND B.CALC_FUNC_C='SPOT' 
AND B.TIME_PERD_C = 'E' 
/*reconciling the balances only for the current record*/
AND CURRENT_DATE BETWEEN B.EFFT_D AND B.EXPY_D
)DT1
INNER JOIN
%%DDSTG%%.ACCT_BALN_BKDT_STG2 STG
ON
DT1.ACCT_I=STG.ACCT_I
AND CURRENT_DATE BETWEEN STG.BKDT_EFFT_D AND STG.BKDT_EXPY_D
;


/*Activity Count > 0 implies - An adjustment affecting the balances of an open record for 
the ACCT_I loaded into ACCT_BALN_BKDT_RECN table.This impacts the next daily delta 
load of Datastage. Thereby it is highly recommended to fix the issue 
before you restart in such a failures*/

.IF ACTIVITYCOUNT<>0 THEN .GOTO ERR_SEV
```

#### 🔍 Syntax Validation Details:
- **Valid**: ❌

#### 🎯 Teradata Features Detected:
- Variable substitution

### SQL Block 4 (Lines 202-212)
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
%%VTECH%%.ACCT_BALN_BKDT_RECN)A(INS_CNT)
SET     COMT_F = 'Y',
	SUCC_F='Y',
	COMT_S =  CURRENT_TIMESTAMP(0),
	SYST_INS_Q = A.INS_CNT
 WHERE PROS_KEY_I = (SELECT MAX(PROS_KEY_I) FROM %%VTECH%%.UTIL_PROS_ISAC 
 WHERE CONV_M='CAD_X01_ACCT_BALN_BKDT_RECN') ;
```

#### ❄️ Converted Snowflake SQL:
```sql
UPDATE %%CAD_PROD_DATA%%.UTIL_PROS_ISAC SET COMT_F = 'Y', SUCC_F = 'Y', COMT_S = CURRENT_TIMESTAMP(0), SYST_INS_Q = A.INS_CNT FROM (SELECT COUNT(*) FROM %%VTECH%%.ACCT_BALN_BKDT_RECN) AS A(INS_CNT) WHERE PROS_KEY_I = (SELECT MAX(PROS_KEY_I) FROM %%VTECH%%.UTIL_PROS_ISAC WHERE CONV_M = 'CAD_X01_ACCT_BALN_BKDT_RECN')
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
  FROM "%%vtech%%"."acct_baln_bkdt_recn" AS "acct_baln_bkdt_recn"
) AS "a"("ins_cnt")
WHERE
  "pros_key_i" = (
    SELECT
      MAX("util_pros_isac"."pros_key_i") AS "_col_0"
    FROM "%%vtech%%"."util_pros_isac" AS "util_pros_isac"
    WHERE
      "util_pros_isac"."conv_m" = 'CAD_X01_ACCT_BALN_BKDT_RECN'
  )
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_BALN_BKDT_RECN, UTIL_PROS_ISAC
- **Columns**: COMT_F, COMT_S, CONV_M, INS_CNT, PROS_KEY_I, SUCC_F, SYST_INS_Q
- **Functions**: *, 0, PROS_KEY_I

### SQL Block 5 (Lines 228-235)
- **Complexity Score**: 0
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ❌ Invalid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
UPDATE %%CAD_PROD_DATA%%.ACCT_BALN_BKDT_RECN
FROM
(SELECT MAX(PROS_KEY_I) AS PROS_KEY_I
FROM %%VTECH%%.UTIL_PROS_ISAC 
WHERE CONV_M= 'CAD_X01_ACCT_BALN_BKDT_RECN') D (PROS_KEY_I)
SET PROS_KEY_EFFT_I = D.PROS_KEY_I;

.QUIT 1
```

#### ❄️ Converted Snowflake SQL:
```sql
UPDATE %%CAD_PROD_DATA%%.ACCT_BALN_BKDT_RECN
FROM
(SELECT MAX(PROS_KEY_I) AS PROS_KEY_I
FROM %%VTECH%%.UTIL_PROS_ISAC 
WHERE CONV_M= 'CAD_X01_ACCT_BALN_BKDT_RECN') D (PROS_KEY_I)
SET PROS_KEY_EFFT_I = D.PROS_KEY_I;

.QUIT 1
```

#### 🔍 Syntax Validation Details:
- **Valid**: ❌

#### 🎯 Teradata Features Detected:
- Variable substitution

## Migration Recommendations

### Suggested Migration Strategy
**Medium complexity** - Incremental model with DCF hooks recommended

### DCF Mapping
- **Error Handling**: Convert `.IF ERRORCODE` patterns to DCF error handling macros
- **File Operations**: Replace IMPORT/EXPORT with dbt seeds or external tables
- **Statistics**: Replace COLLECT STATS with Snowflake post-hooks
- **Stored Procedures**: Map CALL statements to DCF stored procedure macros

---

*Generated by BTEQ Parser Service - 2025-08-20 12:39:25*
