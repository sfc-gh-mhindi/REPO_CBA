# prtf_tech_own_rel_psst.sql - BTEQ Analysis

## File Overview
- **File Name**: prtf_tech_own_rel_psst.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 11
- **SQL Blocks**: 2

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 2 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 33 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 37 | COLLECT_STATS | `COLLECT STATS  %%STARDATADB%%.DERV_PRTF_OWN_REL;` |
| 40 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 120 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 122 | COLLECT_STATS | `COLLECT STATS  %%STARDATADB%%.DERV_PRTF_OWN_REL;` |
| 124 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 127 | LOGOFF | `.LOGOFF` |
| 129 | LABEL | `.LABEL EXITERR` |
| 131 | LOGOFF | `.LOGOFF` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 31-32)
- **Complexity Score**: 6
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Delete from %%STARDATADB%%.DERV_PRTF_OWN_REL All
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%STARDATADB%%.DERV_PRTF_OWN_REL AS All
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%stardatadb%%"."derv_prtf_own_rel" AS "all"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_OWN_REL

### SQL Block 2 (Lines 42-119)
- **Complexity Score**: 270
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_OWN_REL
(
    INT_GRUP_I 
  , DERV_PRTF_CATG_C 
  , DERV_PRTF_CLAS_C 
  , DERV_PRTF_TYPE_C 
  , VALD_FROM_D 
  , VALD_TO_D 
  , EFFT_D 
  , EXPY_D 
  , PTCL_N 
  , REL_MNGE_I 
  , PRTF_CODE_X 
  , DERV_PRTF_ROLE_C 
  , ROLE_PLAY_TYPE_X 
  , ROLE_PLAY_I
  , SRCE_SYST_C 
  , ROW_SECU_ACCS_C 
)
SELECT 
    DT1.INT_GRUP_I
  , GPTE2.PRTF_CATG_C AS DERV_PRTF_CATG_C 
  , GPTE2.PRTF_CLAS_C AS DERV_PRTF_CLAS_C
  , DT1.DERV_PRTF_TYPE_C AS DERV_PRTF_TYPE_C 
  , DT1.VALD_FROM_D
  , DT1.VALD_TO_D
  , DT1.EFFT_D AS EFFT_D
  , DT1.EXPY_D AS EXPY_D 
  , DT1.PTCL_N AS PTCL_N
  , DT1.REL_MNGE_I AS REL_MNGE_I
  , DT1.PRTF_CODE_X AS PRTF_CODE_X 
  , DT1.DERV_PRTF_ROLE_C AS DERV_PRTF_ROLE_C
  , DT1.ROLE_PLAY_TYPE_X AS ROLE_PLAY_TYPE_X
  , DT1.ROLE_PLAY_I AS ROLE_PLAY_I 
  , DT1.SRCE_SYST_C AS SRCE_SYST_C 
  , DT1.ROW_SECU_ACCS_C AS ROW_SECU_ACCS_C 
FROM 
    ( SELECT 
          IG2.INT_GRUP_I AS INT_GRUP_I
        , IGED.EFFT_D AS EFFT_D 
        , IGED.EXPY_D AS EXPY_D 
        , IG2.INT_GRUP_TYPE_C AS DERV_PRTF_TYPE_C
        , CAST( IG2.PTCL_N AS SMALLINT ) AS PTCL_N
        , IG2.REL_MNGE_I AS REL_MNGE_I
        , ( CASE 
               WHEN ( IG2.PTCL_N IS NULL ) OR ( IG2.REL_MNGE_I IS NULL ) THEN 'NA'
               ELSE TRIM ( IG2.PTCL_N ) || TRIM ( IG2.REL_MNGE_I ) 
               END  ) AS PRTF_CODE_X
        , IGED.DERV_PRTF_ROLE_C AS DERV_PRTF_ROLE_C 
        , IGED.ROLE_PLAY_TYPE_X AS ROLE_PLAY_TYPE_X 
        , IGED.ROLE_PLAY_I AS ROLE_PLAY_I 
        , IGED.SRCE_SYST_C AS SRCE_SYST_C
        , IGED.ROW_SECU_ACCS_C AS ROW_SECU_ACCS_C 
        , ( CASE 
               WHEN IG2.JOIN_FROM_D > IGED.JOIN_FROM_D 
               THEN IG2.JOIN_FROM_D 
               ELSE IGED.JOIN_FROM_D
               END  ) AS VALD_FROM_D 
        , ( CASE 
               WHEN IG2.JOIN_TO_D < IGED.JOIN_TO_D 
               THEN IG2.JOIN_TO_D 
               ELSE IGED.JOIN_TO_D
               END  ) AS VALD_TO_D 
                       
   FROM %%VTECH%%.DERV_PRTF_OWN_PSST IGED

   INNER JOIN %%VTECH%%.DERV_PRTF_INT_PSST IG2
           ON IGED.INT_GRUP_I = IG2.INT_GRUP_I
          AND IGED.JOIN_TO_D >= IG2.JOIN_FROM_D 
          AND IGED.JOIN_FROM_D <= IG2.JOIN_TO_D 
) DT1 
                          
INNER JOIN %%VTECH%%.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE2 
        ON GPTE2.PRTF_TYPE_C = DT1.DERV_PRTF_TYPE_C 

;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%STARDATADB%%.DERV_PRTF_OWN_REL (INT_GRUP_I, DERV_PRTF_CATG_C, DERV_PRTF_CLAS_C, DERV_PRTF_TYPE_C, VALD_FROM_D, VALD_TO_D, EFFT_D, EXPY_D, PTCL_N, REL_MNGE_I, PRTF_CODE_X, DERV_PRTF_ROLE_C, ROLE_PLAY_TYPE_X, ROLE_PLAY_I, SRCE_SYST_C, ROW_SECU_ACCS_C) SELECT DT1.INT_GRUP_I, GPTE2.PRTF_CATG_C AS DERV_PRTF_CATG_C, GPTE2.PRTF_CLAS_C AS DERV_PRTF_CLAS_C, DT1.DERV_PRTF_TYPE_C AS DERV_PRTF_TYPE_C, DT1.VALD_FROM_D, DT1.VALD_TO_D, DT1.EFFT_D AS EFFT_D, DT1.EXPY_D AS EXPY_D, DT1.PTCL_N AS PTCL_N, DT1.REL_MNGE_I AS REL_MNGE_I, DT1.PRTF_CODE_X AS PRTF_CODE_X, DT1.DERV_PRTF_ROLE_C AS DERV_PRTF_ROLE_C, DT1.ROLE_PLAY_TYPE_X AS ROLE_PLAY_TYPE_X, DT1.ROLE_PLAY_I AS ROLE_PLAY_I, DT1.SRCE_SYST_C AS SRCE_SYST_C, DT1.ROW_SECU_ACCS_C AS ROW_SECU_ACCS_C FROM (SELECT IG2.INT_GRUP_I AS INT_GRUP_I, IGED.EFFT_D AS EFFT_D, IGED.EXPY_D AS EXPY_D, IG2.INT_GRUP_TYPE_C AS DERV_PRTF_TYPE_C, CAST(IG2.PTCL_N AS SMALLINT) AS PTCL_N, IG2.REL_MNGE_I AS REL_MNGE_I, (CASE WHEN (IG2.PTCL_N IS NULL) OR (IG2.REL_MNGE_I IS NULL) THEN 'NA' ELSE TRIM(IG2.PTCL_N) || TRIM(IG2.REL_MNGE_I) END) AS PRTF_CODE_X, IGED.DERV_PRTF_ROLE_C AS DERV_PRTF_ROLE_C, IGED.ROLE_PLAY_TYPE_X AS ROLE_PLAY_TYPE_X, IGED.ROLE_PLAY_I AS ROLE_PLAY_I, IGED.SRCE_SYST_C AS SRCE_SYST_C, IGED.ROW_SECU_ACCS_C AS ROW_SECU_ACCS_C, (CASE WHEN IG2.JOIN_FROM_D > IGED.JOIN_FROM_D THEN IG2.JOIN_FROM_D ELSE IGED.JOIN_FROM_D END) AS VALD_FROM_D, (CASE WHEN IG2.JOIN_TO_D < IGED.JOIN_TO_D THEN IG2.JOIN_TO_D ELSE IGED.JOIN_TO_D END) AS VALD_TO_D FROM %%VTECH%%.DERV_PRTF_OWN_PSST AS IGED INNER JOIN %%VTECH%%.DERV_PRTF_INT_PSST AS IG2 ON IGED.INT_GRUP_I = IG2.INT_GRUP_I AND IGED.JOIN_TO_D >= IG2.JOIN_FROM_D AND IGED.JOIN_FROM_D <= IG2.JOIN_TO_D) AS DT1 INNER JOIN %%VTECH%%.GRD_PRTF_TYPE_ENHC_HIST_PSST AS GPTE2 ON GPTE2.PRTF_TYPE_C = DT1.DERV_PRTF_TYPE_C
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%stardatadb%%"."derv_prtf_own_rel" (
  "int_grup_i",
  "derv_prtf_catg_c",
  "derv_prtf_clas_c",
  "derv_prtf_type_c",
  "vald_from_d",
  "vald_to_d",
  "efft_d",
  "expy_d",
  "ptcl_n",
  "rel_mnge_i",
  "prtf_code_x",
  "derv_prtf_role_c",
  "role_play_type_x",
  "role_play_i",
  "srce_syst_c",
  "row_secu_accs_c"
)
SELECT
  "ig2"."int_grup_i" AS "int_grup_i",
  "gpte2"."prtf_catg_c" AS "derv_prtf_catg_c",
  "gpte2"."prtf_clas_c" AS "derv_prtf_clas_c",
  "ig2"."int_grup_type_c" AS "derv_prtf_type_c",
  CASE
    WHEN "ig2"."join_from_d" > "iged"."join_from_d"
    THEN "ig2"."join_from_d"
    ELSE "iged"."join_from_d"
  END AS "vald_from_d",
  CASE
    WHEN "ig2"."join_to_d" < "iged"."join_to_d"
    THEN "ig2"."join_to_d"
    ELSE "iged"."join_to_d"
  END AS "vald_to_d",
  "iged"."efft_d" AS "efft_d",
  "iged"."expy_d" AS "expy_d",
  CAST("ig2"."ptcl_n" AS SMALLINT) AS "ptcl_n",
  "ig2"."rel_mnge_i" AS "rel_mnge_i",
  CASE
    WHEN "ig2"."ptcl_n" IS NULL OR "ig2"."rel_mnge_i" IS NULL
    THEN 'NA'
    ELSE TRIM("ig2"."ptcl_n") || TRIM("ig2"."rel_mnge_i")
  END AS "prtf_code_x",
  "iged"."derv_prtf_role_c" AS "derv_prtf_role_c",
  "iged"."role_play_type_x" AS "role_play_type_x",
  "iged"."role_play_i" AS "role_play_i",
  "iged"."srce_syst_c" AS "srce_syst_c",
  "iged"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "%%vtech%%"."derv_prtf_own_psst" AS "iged"
JOIN "%%vtech%%"."derv_prtf_int_psst" AS "ig2"
  ON "ig2"."int_grup_i" = "iged"."int_grup_i"
  AND "ig2"."join_from_d" <= "iged"."join_to_d"
  AND "ig2"."join_to_d" >= "iged"."join_from_d"
JOIN "%%vtech%%"."grd_prtf_type_enhc_hist_psst" AS "gpte2"
  ON "gpte2"."prtf_type_c" = "ig2"."int_grup_type_c"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_INT_PSST, DERV_PRTF_OWN_PSST, DERV_PRTF_OWN_REL, GRD_PRTF_TYPE_ENHC_HIST_PSST
- **Columns**: DERV_PRTF_ROLE_C, DERV_PRTF_TYPE_C, EFFT_D, EXPY_D, INT_GRUP_I, INT_GRUP_TYPE_C, JOIN_FROM_D, JOIN_TO_D, PRTF_CATG_C, PRTF_CLAS_C, PRTF_CODE_X, PRTF_TYPE_C, PTCL_N, REL_MNGE_I, ROLE_PLAY_I, ROLE_PLAY_TYPE_X, ROW_SECU_ACCS_C, SRCE_SYST_C, VALD_FROM_D, VALD_TO_D
- **Functions**: (IG2.PTCL_N IS NULL), (IG2.PTCL_N IS NULL) OR (IG2.REL_MNGE_I IS NULL), IG2.JOIN_FROM_D > IGED.JOIN_FROM_D, IG2.JOIN_TO_D < IGED.JOIN_TO_D, IG2.PTCL_N, IG2.REL_MNGE_I, IGED.INT_GRUP_I = IG2.INT_GRUP_I, IGED.INT_GRUP_I = IG2.INT_GRUP_I AND IGED.JOIN_TO_D >= IG2.JOIN_FROM_D, None

## Migration Recommendations

### Suggested Migration Strategy
**High complexity** - Break into multiple models, use DCF full framework

### DCF Mapping
- **Error Handling**: Convert `.IF ERRORCODE` patterns to DCF error handling macros
- **File Operations**: Replace IMPORT/EXPORT with dbt seeds or external tables
- **Statistics**: Replace COLLECT STATS with Snowflake post-hooks
- **Stored Procedures**: Map CALL statements to DCF stored procedure macros

---

*Generated by BTEQ Parser Service - 2025-08-20 12:39:23*
