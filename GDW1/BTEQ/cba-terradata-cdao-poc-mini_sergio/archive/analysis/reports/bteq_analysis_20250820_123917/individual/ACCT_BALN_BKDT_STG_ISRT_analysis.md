# ACCT_BALN_BKDT_STG_ISRT.sql - BTEQ Analysis

## File Overview
- **File Name**: ACCT_BALN_BKDT_STG_ISRT.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 9
- **SQL Blocks**: 4

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 2 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 28 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 81 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 88 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 164 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 167 | LOGOFF | `.LOGOFF` |
| 171 | LABEL | `.LABEL EXITERR` |
| 173 | LOGOFF | `.LOGOFF` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 26-27)
- **Complexity Score**: 4
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
DELETE %%DDSTG%%.ACCT_BALN_BKDT_STG1;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE %%DDSTG%%.ACCT_BALN_BKDT_STG1
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE   "%%ddstg%%"."acct_baln_bkdt_stg1"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_BALN_BKDT_STG1

### SQL Block 2 (Lines 30-80)
- **Complexity Score**: 101
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT	INTO  %%DDSTG%%.ACCT_BALN_BKDT_STG1
(
ACCT_I,
BALN_TYPE_C,
CALC_FUNC_C,
TIME_PERD_C,
BKDT_EFFT_D,
BKDT_EXPY_D,
BALN_A,
CALC_F,
PROS_KEY_EFFT_I,
PROS_KEY_EXPY_I,
BKDT_PROS_KEY_I,
ORIG_SRCE_SYST_C,
SRCE_SYST_C,
LOAD_D
)
SELECT	
A.ACCT_I,
A.BALN_TYPE_C,
A.CALC_FUNC_C,                  
A.TIME_PERD_C,                  
A.BKDT_EFFT_D,
A.BKDT_EXPY_D,
A.BALN_A,
A.CALC_F,
A.PROS_KEY_EFFT_I,
A.PROS_KEY_EXPY_I,
A.BKDT_PROS_KEY_I,
A.ORIG_SRCE_SYST_C,
A.SRCE_SYST_C,
A.LOAD_D
FROM	
%%VTECH%%.ACCT_BALN_BKDT  A
INNER JOIN
/*Identify suitable candidates for processing. 
This avoids pulling entire history into staging environment*/
(
SELECT	
ACCT_I, 
MIN(BKDT_ADJ_FROM_D) AS BKDT_ADJ_FROM_D
FROM	
%%DDSTG%%.ACCT_BALN_BKDT_ADJ_RULE
GROUP	BY 1
)B
ON	
A.ACCT_I=B.ACCT_I
WHERE	
A.BKDT_EXPY_D >= B.BKDT_ADJ_FROM_D
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.ACCT_BALN_BKDT_STG1 (ACCT_I, BALN_TYPE_C, CALC_FUNC_C, TIME_PERD_C, BKDT_EFFT_D, BKDT_EXPY_D, BALN_A, CALC_F, PROS_KEY_EFFT_I, PROS_KEY_EXPY_I, BKDT_PROS_KEY_I, ORIG_SRCE_SYST_C, SRCE_SYST_C, LOAD_D) SELECT A.ACCT_I, A.BALN_TYPE_C, A.CALC_FUNC_C, A.TIME_PERD_C, A.BKDT_EFFT_D, A.BKDT_EXPY_D, A.BALN_A, A.CALC_F, A.PROS_KEY_EFFT_I, A.PROS_KEY_EXPY_I, A.BKDT_PROS_KEY_I, A.ORIG_SRCE_SYST_C, A.SRCE_SYST_C, A.LOAD_D FROM %%VTECH%%.ACCT_BALN_BKDT AS A INNER JOIN (SELECT ACCT_I, MIN(BKDT_ADJ_FROM_D) AS BKDT_ADJ_FROM_D FROM %%DDSTG%%.ACCT_BALN_BKDT_ADJ_RULE GROUP BY 1) AS B ON A.ACCT_I = B.ACCT_I WHERE A.BKDT_EXPY_D >= B.BKDT_ADJ_FROM_D
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."acct_baln_bkdt_stg1" (
  "acct_i",
  "baln_type_c",
  "calc_func_c",
  "time_perd_c",
  "bkdt_efft_d",
  "bkdt_expy_d",
  "baln_a",
  "calc_f",
  "pros_key_efft_i",
  "pros_key_expy_i",
  "bkdt_pros_key_i",
  "orig_srce_syst_c",
  "srce_syst_c",
  "load_d"
)
WITH "b" AS (
  SELECT
    "acct_baln_bkdt_adj_rule"."acct_i" AS "acct_i",
    MIN("acct_baln_bkdt_adj_rule"."bkdt_adj_from_d") AS "bkdt_adj_from_d"
  FROM "%%ddstg%%"."acct_baln_bkdt_adj_rule" AS "acct_baln_bkdt_adj_rule"
  GROUP BY
    "acct_baln_bkdt_adj_rule"."acct_i"
)
SELECT
  "a"."acct_i" AS "acct_i",
  "a"."baln_type_c" AS "baln_type_c",
  "a"."calc_func_c" AS "calc_func_c",
  "a"."time_perd_c" AS "time_perd_c",
  "a"."bkdt_efft_d" AS "bkdt_efft_d",
  "a"."bkdt_expy_d" AS "bkdt_expy_d",
  "a"."baln_a" AS "baln_a",
  "a"."calc_f" AS "calc_f",
  "a"."pros_key_efft_i" AS "pros_key_efft_i",
  "a"."pros_key_expy_i" AS "pros_key_expy_i",
  "a"."bkdt_pros_key_i" AS "bkdt_pros_key_i",
  "a"."orig_srce_syst_c" AS "orig_srce_syst_c",
  "a"."srce_syst_c" AS "srce_syst_c",
  "a"."load_d" AS "load_d"
FROM "%%vtech%%"."acct_baln_bkdt" AS "a"
JOIN "b" AS "b"
  ON "a"."acct_i" = "b"."acct_i" AND "a"."bkdt_expy_d" >= "b"."bkdt_adj_from_d"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_BALN_BKDT, ACCT_BALN_BKDT_ADJ_RULE, ACCT_BALN_BKDT_STG1
- **Columns**: ACCT_I, BALN_A, BALN_TYPE_C, BKDT_ADJ_FROM_D, BKDT_EFFT_D, BKDT_EXPY_D, BKDT_PROS_KEY_I, CALC_F, CALC_FUNC_C, LOAD_D, ORIG_SRCE_SYST_C, PROS_KEY_EFFT_I, PROS_KEY_EXPY_I, SRCE_SYST_C, TIME_PERD_C
- **Functions**: BKDT_ADJ_FROM_D

### SQL Block 3 (Lines 86-87)
- **Complexity Score**: 4
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
DELETE	 %%DDSTG%%.ACCT_BALN_BKDT_STG2;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE %%DDSTG%%.ACCT_BALN_BKDT_STG2
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE   "%%ddstg%%"."acct_baln_bkdt_stg2"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_BALN_BKDT_STG2

### SQL Block 4 (Lines 90-163)
- **Complexity Score**: 303
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 2

#### 📝 Original Teradata SQL:
```sql
INSERT	INTO   %%DDSTG%%.ACCT_BALN_BKDT_STG2
SELECT DISTINCT	
DT1.ACCT_I       AS ACCT_I
/*The BALN_TYPE_C in ACCT_BALN_BKDT table is hardcoded to 'BDCL'*/
,COALESCE(B.BALN_TYPE_C,'BDCL') 
,COALESCE(B.CALC_FUNC_C,'SPOT')              
,COALESCE(B.TIME_PERD_C,'E')
,DT1.BKDT_EFFT_D AS BKDT_EFFT_D
,DT1.BKDT_EXPY_D AS BKDT_EXPY_D
/* Calculate the adjusted balance value as the sum of all relevant adjustments plus the relevant balance value */
/* Note that MAX is used for BAL amount simply to identify the single balance valid during the time period */
,MAX(COALESCE(B.BALN_A,0.0)) OVER 
(PARTITION BY DT1.ACCT_I,DT1.BKDT_EFFT_D,DT1.BKDT_EXPY_D) 
+ SUM(COALESCE(A.ADJ_A,0.0)) OVER 
(PARTITION BY DT1.ACCT_I, DT1.BKDT_EFFT_D,DT1.BKDT_EXPY_D)    AS BALN_A
,COALESCE(B.CALC_F,'N')
,B.PROS_KEY_EFFT_I
,B.PROS_KEY_EXPY_I
,PKEY.BKDT_PROS_KEY_I AS BKDT_PROS_KEY_I
,A.PROS_KEY_EFFT_I AS ADJ_PROS_KEY_EFFT_I
,COALESCE(B.ORIG_SRCE_SYST_C,'SAP')
,COALESCE(B.SRCE_SYST_C,'GDW')
,CURRENT_DATE AS LOAD_D
/*  Identify ALL TIME periods OF interest FOR EACH ACCT_I */
FROM	
	   (SELECT	DT0.ACCT_I
	   /* Calculate start points of time periods pf interest based on end point of previous time period */
      ,(MAX(DT0.BKDT_EXPY_D) OVER (PARTITION BY DT0.ACCT_I ORDER
      	BY DT0.BKDT_EXPY_D ROWS BETWEEN 1 PRECEDING 
      	AND	1 PRECEDING) + 1)  AS BKDT_EFFT_D  , DT0.BKDT_EXPY_D
      	FROM	 (
      	/*  Identify END points OF ALL TIME periods OF interest FOR EACH  ACCT_I*/	               
		SELECT ACCT_I, BKDT_EXPY_D          
		FROM %%DDSTG%%.ACCT_BALN_BKDT_STG1
		UNION	 	            
		SELECT ACCT_I,ADJ_TO_D            
		FROM %%DDSTG%%.ACCT_BALN_BKDT_ADJ_RULE
		UNION	                
		SELECT	ACCT_I,BKDT_EFFT_D - 1                
		FROM	%%DDSTG%%.ACCT_BALN_BKDT_STG1
		UNION	                
		SELECT	 ACCT_I,BKDT_ADJ_FROM_D - 1             
		FROM	%%DDSTG%%.ACCT_BALN_BKDT_ADJ_RULE) DT0 (ACCT_I,BKDT_EXPY_D)
		/* Ignore record where there is no start point */
		QUALIFY	BKDT_EFFT_D IS NOT NULL
	) DT1
 /* Join to balance table based on ACCT_I and intersection with time periods of interest */
/* Note that there may be no balance amount related to a time period */
			
LEFT OUTER JOIN
%%DDSTG%%.ACCT_BALN_BKDT_STG1  B
ON DT1.ACCT_I = B.ACCT_I
AND(
(DT1.BKDT_EFFT_D, DT1.BKDT_EXPY_D) OVERLAPS (B.BKDT_EFFT_D,B.BKDT_EXPY_D)
/* as Overlaps does not include equality*/
OR DT1.BKDT_EFFT_D = B.BKDT_EFFT_D
OR DT1.BKDT_EXPY_D = B.BKDT_EXPY_D
)
/* Join to adjustment table based on ACCT_I and intersection with time periods of interest */
/* Note that there may be multiple adjustments or no adjustments related to a time period */

LEFT OUTER JOIN
%%DDSTG%%.ACCT_BALN_BKDT_ADJ_RULE A
ON DT1.ACCT_I = A.ACCT_I                   
AND (
(DT1.BKDT_EFFT_D, DT1.BKDT_EXPY_D) OVERLAPS (A.BKDT_ADJ_FROM_D,A.ADJ_TO_D)
OR DT1.BKDT_EFFT_D = A.BKDT_ADJ_FROM_D
OR DT1.BKDT_EXPY_D = A.ADJ_TO_D)
CROSS JOIN
/*update the latest PROS_KEY_I into subsequent inserts*/
(SELECT MAX(PROS_KEY_I)  AS BKDT_PROS_KEY_I
FROM %%VTECH%%.UTIL_PROS_ISAC WHERE 
CONV_M='CAD_X01_ACCT_BALN_BKDT')PKEY;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.ACCT_BALN_BKDT_STG2 SELECT DISTINCT DT1.ACCT_I AS ACCT_I, COALESCE(B.BALN_TYPE_C, 'BDCL'), COALESCE(B.CALC_FUNC_C, 'SPOT'), COALESCE(B.TIME_PERD_C, 'E'), DT1.BKDT_EFFT_D AS BKDT_EFFT_D, DT1.BKDT_EXPY_D AS BKDT_EXPY_D, MAX(COALESCE(B.BALN_A, 0.0)) OVER (PARTITION BY DT1.ACCT_I, DT1.BKDT_EFFT_D, DT1.BKDT_EXPY_D) + SUM(COALESCE(A.ADJ_A, 0.0)) OVER (PARTITION BY DT1.ACCT_I, DT1.BKDT_EFFT_D, DT1.BKDT_EXPY_D) AS BALN_A, COALESCE(B.CALC_F, 'N'), B.PROS_KEY_EFFT_I, B.PROS_KEY_EXPY_I, PKEY.BKDT_PROS_KEY_I AS BKDT_PROS_KEY_I, A.PROS_KEY_EFFT_I AS ADJ_PROS_KEY_EFFT_I, COALESCE(B.ORIG_SRCE_SYST_C, 'SAP'), COALESCE(B.SRCE_SYST_C, 'GDW'), CURRENT_DATE AS LOAD_D FROM (SELECT DT0.ACCT_I, (MAX(DT0.BKDT_EXPY_D) OVER (PARTITION BY DT0.ACCT_I ORDER BY DT0.BKDT_EXPY_D NULLS FIRST ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) + 1) AS BKDT_EFFT_D, DT0.BKDT_EXPY_D FROM (SELECT ACCT_I, BKDT_EXPY_D FROM %%DDSTG%%.ACCT_BALN_BKDT_STG1 UNION SELECT ACCT_I, ADJ_TO_D FROM %%DDSTG%%.ACCT_BALN_BKDT_ADJ_RULE UNION SELECT ACCT_I, BKDT_EFFT_D - 1 FROM %%DDSTG%%.ACCT_BALN_BKDT_STG1 UNION SELECT ACCT_I, BKDT_ADJ_FROM_D - 1 FROM %%DDSTG%%.ACCT_BALN_BKDT_ADJ_RULE) AS DT0(ACCT_I, BKDT_EXPY_D) QUALIFY NOT BKDT_EFFT_D IS NULL) AS DT1 LEFT OUTER JOIN %%DDSTG%%.ACCT_BALN_BKDT_STG1 AS B ON DT1.ACCT_I = B.ACCT_I AND ((DT1.BKDT_EFFT_D, DT1.BKDT_EXPY_D) OVERLAPS (B.BKDT_EFFT_D, B.BKDT_EXPY_D) OR DT1.BKDT_EFFT_D = B.BKDT_EFFT_D OR DT1.BKDT_EXPY_D = B.BKDT_EXPY_D) LEFT OUTER JOIN %%DDSTG%%.ACCT_BALN_BKDT_ADJ_RULE AS A ON DT1.ACCT_I = A.ACCT_I AND ((DT1.BKDT_EFFT_D, DT1.BKDT_EXPY_D) OVERLAPS (A.BKDT_ADJ_FROM_D, A.ADJ_TO_D) OR DT1.BKDT_EFFT_D = A.BKDT_ADJ_FROM_D OR DT1.BKDT_EXPY_D = A.ADJ_TO_D) CROSS JOIN (SELECT MAX(PROS_KEY_I) AS BKDT_PROS_KEY_I FROM %%VTECH%%.UTIL_PROS_ISAC WHERE CONV_M = 'CAD_X01_ACCT_BALN_BKDT') AS PKEY
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- QUALIFY clause
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."acct_baln_bkdt_stg2"
WITH "dt0" AS (
  SELECT
    "acct_baln_bkdt_stg1"."acct_i" AS "acct_i",
    "acct_baln_bkdt_stg1"."bkdt_expy_d" AS "bkdt_expy_d"
  FROM "%%ddstg%%"."acct_baln_bkdt_stg1" AS "acct_baln_bkdt_stg1"
  UNION
  SELECT
    "acct_baln_bkdt_adj_rule"."acct_i" AS "acct_i",
    "acct_baln_bkdt_adj_rule"."adj_to_d" AS "bkdt_expy_d"
  FROM "%%ddstg%%"."acct_baln_bkdt_adj_rule" AS "acct_baln_bkdt_adj_rule"
  UNION
  SELECT
    "acct_baln_bkdt_stg1"."acct_i" AS "acct_i",
    "acct_baln_bkdt_stg1"."bkdt_efft_d" - 1 AS "bkdt_expy_d"
  FROM "%%ddstg%%"."acct_baln_bkdt_stg1" AS "acct_baln_bkdt_stg1"
  UNION
  SELECT
    "acct_baln_bkdt_adj_rule"."acct_i" AS "acct_i",
    "acct_baln_bkdt_adj_rule"."bkdt_adj_from_d" - 1 AS "bkdt_expy_d"
  FROM "%%ddstg%%"."acct_baln_bkdt_adj_rule" AS "acct_baln_bkdt_adj_rule"
), "dt1" AS (
  SELECT
    "dt0"."acct_i" AS "acct_i",
    MAX("dt0"."bkdt_expy_d") OVER (
      PARTITION BY "dt0"."acct_i"
      ORDER BY "dt0"."bkdt_expy_d" NULLS FIRST
      ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
    ) + 1 AS "bkdt_efft_d",
    "dt0"."bkdt_expy_d" AS "bkdt_expy_d"
  FROM "dt0" AS "dt0"
  QUALIFY
    NOT (
      MAX("dt0"."bkdt_expy_d") OVER (
        PARTITION BY "dt0"."acct_i"
        ORDER BY "dt0"."bkdt_expy_d" NULLS FIRST
        ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING
      ) + 1
    ) IS NULL
), "pkey" AS (
  SELECT
    MAX("util_pros_isac"."pros_key_i") AS "bkdt_pros_key_i"
  FROM "%%vtech%%"."util_pros_isac" AS "util_pros_isac"
  WHERE
    "util_pros_isac"."conv_m" = 'CAD_X01_ACCT_BALN_BKDT'
)
SELECT DISTINCT
  "dt1"."acct_i" AS "acct_i",
  COALESCE("b"."baln_type_c", 'BDCL') AS "_col_1",
  COALESCE("b"."calc_func_c", 'SPOT') AS "_col_2",
  COALESCE("b"."time_perd_c", 'E') AS "_col_3",
  "dt1"."bkdt_efft_d" AS "bkdt_efft_d",
  "dt1"."bkdt_expy_d" AS "bkdt_expy_d",
  MAX(COALESCE("b"."baln_a", 0.0)) OVER (PARTITION BY "dt1"."acct_i", "dt1"."bkdt_efft_d", "dt1"."bkdt_expy_d") + SUM(COALESCE("a"."adj_a", 0.0)) OVER (PARTITION BY "dt1"."acct_i", "dt1"."bkdt_efft_d", "dt1"."bkdt_expy_d") AS "baln_a",
  COALESCE("b"."calc_f", 'N') AS "_col_7",
  "b"."pros_key_efft_i" AS "pros_key_efft_i",
  "b"."pros_key_expy_i" AS "pros_key_expy_i",
  "pkey"."bkdt_pros_key_i" AS "bkdt_pros_key_i",
  "a"."pros_key_efft_i" AS "adj_pros_key_efft_i",
  COALESCE("b"."orig_srce_syst_c", 'SAP') AS "_col_12",
  COALESCE("b"."srce_syst_c", 'GDW') AS "_col_13",
  CURRENT_DATE AS "load_d"
FROM "dt1" AS "dt1"
CROSS JOIN "pkey" AS "pkey"
LEFT JOIN "%%ddstg%%"."acct_baln_bkdt_adj_rule" AS "a"
  ON "a"."acct_i" = "dt1"."acct_i"
  AND (
    "a"."adj_to_d" = "dt1"."bkdt_expy_d"
    OR "a"."bkdt_adj_from_d" = "dt1"."bkdt_efft_d"
    OR ("dt1"."bkdt_efft_d", "dt1"."bkdt_expy_d") OVERLAPS ("a"."bkdt_adj_from_d", "a"."adj_to_d")
  )
LEFT JOIN "%%ddstg%%"."acct_baln_bkdt_stg1" AS "b"
  ON "b"."acct_i" = "dt1"."acct_i"
  AND (
    "b"."bkdt_efft_d" = "dt1"."bkdt_efft_d"
    OR "b"."bkdt_expy_d" = "dt1"."bkdt_expy_d"
    OR ("dt1"."bkdt_efft_d", "dt1"."bkdt_expy_d") OVERLAPS ("b"."bkdt_efft_d", "b"."bkdt_expy_d")
  )
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_BALN_BKDT_ADJ_RULE, ACCT_BALN_BKDT_STG1, ACCT_BALN_BKDT_STG2, UTIL_PROS_ISAC
- **Columns**: ACCT_I, ADJ_A, ADJ_TO_D, BALN_A, BALN_TYPE_C, BKDT_ADJ_FROM_D, BKDT_EFFT_D, BKDT_EXPY_D, BKDT_PROS_KEY_I, CALC_F, CALC_FUNC_C, CONV_M, ORIG_SRCE_SYST_C, PROS_KEY_EFFT_I, PROS_KEY_EXPY_I, PROS_KEY_I, SRCE_SYST_C, TIME_PERD_C
- **Functions**: (DT1.BKDT_EFFT_D, DT1.BKDT_EXPY_D) OVERLAPS (A.BKDT_ADJ_FROM_D, A.ADJ_TO_D), (DT1.BKDT_EFFT_D, DT1.BKDT_EXPY_D) OVERLAPS (A.BKDT_ADJ_FROM_D, A.ADJ_TO_D) OR DT1.BKDT_EFFT_D = A.BKDT_ADJ_FROM_D, (DT1.BKDT_EFFT_D, DT1.BKDT_EXPY_D) OVERLAPS (B.BKDT_EFFT_D, B.BKDT_EXPY_D), (DT1.BKDT_EFFT_D, DT1.BKDT_EXPY_D) OVERLAPS (B.BKDT_EFFT_D, B.BKDT_EXPY_D) OR DT1.BKDT_EFFT_D = B.BKDT_EFFT_D, A.ADJ_A, B.BALN_A, B.BALN_TYPE_C, B.CALC_F, B.CALC_FUNC_C, B.ORIG_SRCE_SYST_C, B.SRCE_SYST_C, B.TIME_PERD_C, COALESCE(A.ADJ_A, 0.0), COALESCE(B.BALN_A, 0.0), DT0.BKDT_EXPY_D, DT1.ACCT_I = A.ACCT_I, DT1.ACCT_I = B.ACCT_I, None, PROS_KEY_I
- **Window_Functions**: COALESCE(A.ADJ_A, 0.0), COALESCE(B.BALN_A, 0.0), DT0.BKDT_EXPY_D

## Migration Recommendations

### Suggested Migration Strategy
**High complexity** - Break into multiple models, use DCF full framework

### DCF Mapping
- **Error Handling**: Convert `.IF ERRORCODE` patterns to DCF error handling macros
- **File Operations**: Replace IMPORT/EXPORT with dbt seeds or external tables
- **Statistics**: Replace COLLECT STATS with Snowflake post-hooks
- **Stored Procedures**: Map CALL statements to DCF stored procedure macros

---

*Generated by BTEQ Parser Service - 2025-08-20 12:39:24*
