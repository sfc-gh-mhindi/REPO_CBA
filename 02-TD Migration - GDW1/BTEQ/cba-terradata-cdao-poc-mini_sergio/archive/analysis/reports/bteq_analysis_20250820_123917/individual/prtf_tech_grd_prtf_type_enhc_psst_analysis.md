# prtf_tech_grd_prtf_type_enhc_psst.sql - BTEQ Analysis

## File Overview
- **File Name**: prtf_tech_grd_prtf_type_enhc_psst.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 12
- **SQL Blocks**: 4

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 2 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 22 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 25 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 39 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 42 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 49 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 65 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 68 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 74 | LOGOFF | `.LOGOFF` |
| 76 | LABEL | `.LABEL EXITERR` |
| 78 | LOGOFF | `.LOGOFF` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 20-21)
- **Complexity Score**: 6
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Delete from %%DGRDDB%%.GRD_PRTF_TYPE_ENHC_PSST All
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%DGRDDB%%.GRD_PRTF_TYPE_ENHC_PSST AS All
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%dgrddb%%"."grd_prtf_type_enhc_psst" AS "all"
```

#### 📊 SQL Metadata:
- **Tables**: GRD_PRTF_TYPE_ENHC_PSST

### SQL Block 2 (Lines 27-38)
- **Complexity Score**: 32
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Insert into %%DGRDDB%%.GRD_PRTF_TYPE_ENHC_PSST
Select
   GP.PERD_D
  ,GP.PRTF_TYPE_C
  ,GP.PRTF_TYPE_M
  ,GP.PRTF_CLAS_C
  ,GP.PRTF_CLAS_M
  ,GP.PRTF_CATG_C
  ,GP.PRTF_CATG_M
From                
  %%VTECH%%.GRD_PRTF_TYPE_ENHC GP
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DGRDDB%%.GRD_PRTF_TYPE_ENHC_PSST SELECT GP.PERD_D, GP.PRTF_TYPE_C, GP.PRTF_TYPE_M, GP.PRTF_CLAS_C, GP.PRTF_CLAS_M, GP.PRTF_CATG_C, GP.PRTF_CATG_M FROM %%VTECH%%.GRD_PRTF_TYPE_ENHC AS GP
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%dgrddb%%"."grd_prtf_type_enhc_psst"
SELECT
  "gp"."perd_d" AS "perd_d",
  "gp"."prtf_type_c" AS "prtf_type_c",
  "gp"."prtf_type_m" AS "prtf_type_m",
  "gp"."prtf_clas_c" AS "prtf_clas_c",
  "gp"."prtf_clas_m" AS "prtf_clas_m",
  "gp"."prtf_catg_c" AS "prtf_catg_c",
  "gp"."prtf_catg_m" AS "prtf_catg_m"
FROM "%%vtech%%"."grd_prtf_type_enhc" AS "gp"
```

#### 📊 SQL Metadata:
- **Tables**: GRD_PRTF_TYPE_ENHC, GRD_PRTF_TYPE_ENHC_PSST
- **Columns**: PERD_D, PRTF_CATG_C, PRTF_CATG_M, PRTF_CLAS_C, PRTF_CLAS_M, PRTF_TYPE_C, PRTF_TYPE_M

### SQL Block 3 (Lines 47-48)
- **Complexity Score**: 4
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Delete from %%DGRDDB%%.GRD_PRTF_TYPE_ENHC_HIST_PSST
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%DGRDDB%%.GRD_PRTF_TYPE_ENHC_HIST_PSST
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%dgrddb%%"."grd_prtf_type_enhc_hist_psst"
```

#### 📊 SQL Metadata:
- **Tables**: GRD_PRTF_TYPE_ENHC_HIST_PSST

### SQL Block 4 (Lines 51-64)
- **Complexity Score**: 46
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Insert into %%DGRDDB%%.GRD_PRTF_TYPE_ENHC_HIST_PSST
Select
   G.PRTF_TYPE_C                   
  ,G.PRTF_TYPE_M                   
  ,G.PRTF_CLAS_C                   
  ,G.PRTF_CLAS_M                   
  ,G.PRTF_CATG_C                   
  ,G.PRTF_CATG_M    
	,MIN(PERD_D) as VALD_FROM_D
  ,MAX(PERD_D) as VALD_TO_D
From
  %%VTECH%%.GRD_PRTF_TYPE_ENHC_PSST G
Group By 1,2,3,4,5,6
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DGRDDB%%.GRD_PRTF_TYPE_ENHC_HIST_PSST SELECT G.PRTF_TYPE_C, G.PRTF_TYPE_M, G.PRTF_CLAS_C, G.PRTF_CLAS_M, G.PRTF_CATG_C, G.PRTF_CATG_M, MIN(PERD_D) AS VALD_FROM_D, MAX(PERD_D) AS VALD_TO_D FROM %%VTECH%%.GRD_PRTF_TYPE_ENHC_PSST AS G GROUP BY 1, 2, 3, 4, 5, 6
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%dgrddb%%"."grd_prtf_type_enhc_hist_psst"
SELECT
  "g"."prtf_type_c" AS "prtf_type_c",
  "g"."prtf_type_m" AS "prtf_type_m",
  "g"."prtf_clas_c" AS "prtf_clas_c",
  "g"."prtf_clas_m" AS "prtf_clas_m",
  "g"."prtf_catg_c" AS "prtf_catg_c",
  "g"."prtf_catg_m" AS "prtf_catg_m",
  MIN("g"."perd_d") AS "vald_from_d",
  MAX("g"."perd_d") AS "vald_to_d"
FROM "%%vtech%%"."grd_prtf_type_enhc_psst" AS "g"
GROUP BY
  "g"."prtf_type_c",
  "g"."prtf_type_m",
  "g"."prtf_clas_c",
  "g"."prtf_clas_m",
  "g"."prtf_catg_c",
  "g"."prtf_catg_m"
```

#### 📊 SQL Metadata:
- **Tables**: GRD_PRTF_TYPE_ENHC_HIST_PSST, GRD_PRTF_TYPE_ENHC_PSST
- **Columns**: PERD_D, PRTF_CATG_C, PRTF_CATG_M, PRTF_CLAS_C, PRTF_CLAS_M, PRTF_TYPE_C, PRTF_TYPE_M
- **Functions**: PERD_D

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
