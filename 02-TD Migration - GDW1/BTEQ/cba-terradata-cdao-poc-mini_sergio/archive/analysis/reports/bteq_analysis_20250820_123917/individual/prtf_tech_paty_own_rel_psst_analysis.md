# prtf_tech_paty_own_rel_psst.sql - BTEQ Analysis

## File Overview
- **File Name**: prtf_tech_paty_own_rel_psst.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 11
- **SQL Blocks**: 2

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 2 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 32 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 36 | COLLECT_STATS | ` COLLECT STATS  %%STARDATADB%%.DERV_PRTF_PATY_OWN_REL;` |
| 38 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 92 | IF_ERRORCODE | `  .IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 94 | COLLECT_STATS | `  COLLECT STATS  %%STARDATADB%%.DERV_PRTF_PATY_OWN_REL;` |
| 96 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 101 | LOGOFF | `.LOGOFF` |
| 103 | LABEL | `.LABEL EXITERR` |
| 105 | LOGOFF | `.LOGOFF` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 30-31)
- **Complexity Score**: 6
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Delete from %%STARDATADB%%.DERV_PRTF_PATY_OWN_REL All
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%STARDATADB%%.DERV_PRTF_PATY_OWN_REL AS All
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%stardatadb%%"."derv_prtf_paty_own_rel" AS "all"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_PATY_OWN_REL

### SQL Block 2 (Lines 40-91)
- **Complexity Score**: 159
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_PATY_OWN_REL
( PATY_I 
, INT_GRUP_I 
, DERV_PRTF_CATG_C
 , DERV_PRTF_CLAS_C 
 , DERV_PRTF_TYPE_C 
 , PRTF_PATY_VALD_FROM_D 
 , PRTF_PATY_VALD_TO_D 
 , PRTF_PATY_EFFT_D 
 , PRTF_PATY_EXPY_D
  , PRTF_OWN_VALD_FROM_D 
  , PRTF_OWN_VALD_TO_D 
  , PRTF_OWN_EFFT_D
   , PRTF_OWN_EXPY_D
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
         PP4.PATY_I AS PATY_I 
       , PP4.INT_GRUP_I AS INT_GRUP_I  
       , PP4.DERV_PRTF_CATG_C AS DERV_PRTF_CATG_C 
       , PP4.DERV_PRTF_CLAS_C AS DERV_PRTF_CLAS_C 
       , PP4.DERV_PRTF_TYPE_C AS DERV_PRTF_TYPE_C 
       , PP4.VALD_FROM_D AS PRTF_PATY_VALD_FROM_D 
       , PP4.VALD_TO_D AS PRTF_PATY_VALD_TO_D 
       , PP4.EFFT_D AS PRTF_PATY_EFFT_D 
       , PP4.EXPY_D AS PRTF_PATY_EXPY_D 
       , PO4.VALD_FROM_D AS PRTF_OWN_VALD_FROM_D 
       , PO4.VALD_TO_D AS PRTF_OWN_VALD_TO_D 
       , PO4.EFFT_D AS PRTF_OWN_EFFT_D
        , PO4.EXPY_D AS PRTF_OWN_EXPY_D 
        , PP4.PTCL_N AS PTCL_N 
        , PP4.REL_MNGE_I AS REL_MNGE_I 
        , PP4.PRTF_CODE_X AS PRTF_CODE_X 
        , PO4.DERV_PRTF_ROLE_C AS DERV_PRTF_ROLE_C
         , PO4.ROLE_PLAY_TYPE_X AS ROLE_PLAY_TYPE_X 
         , PO4.ROLE_PLAY_I AS ROLE_PLAY_I
          , PP4.SRCE_SYST_C AS SRCE_SYST_C 
          , PP4.ROW_SECU_ACCS_C AS ROW_SECU_ACCS_C 

FROM %%VTECH%%.DERV_PRTF_PATY_REL PP4 
INNER JOIN %%VTECH%%.DERV_PRTF_OWN_REL PO4 
ON PO4.INT_GRUP_I = PP4.INT_GRUP_I 
AND PO4.DERV_PRTF_TYPE_C = PP4.DERV_PRTF_TYPE_C ;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%STARDATADB%%.DERV_PRTF_PATY_OWN_REL (PATY_I, INT_GRUP_I, DERV_PRTF_CATG_C, DERV_PRTF_CLAS_C, DERV_PRTF_TYPE_C, PRTF_PATY_VALD_FROM_D, PRTF_PATY_VALD_TO_D, PRTF_PATY_EFFT_D, PRTF_PATY_EXPY_D, PRTF_OWN_VALD_FROM_D, PRTF_OWN_VALD_TO_D, PRTF_OWN_EFFT_D, PRTF_OWN_EXPY_D, PTCL_N, REL_MNGE_I, PRTF_CODE_X, DERV_PRTF_ROLE_C, ROLE_PLAY_TYPE_X, ROLE_PLAY_I, SRCE_SYST_C, ROW_SECU_ACCS_C) SELECT PP4.PATY_I AS PATY_I, PP4.INT_GRUP_I AS INT_GRUP_I, PP4.DERV_PRTF_CATG_C AS DERV_PRTF_CATG_C, PP4.DERV_PRTF_CLAS_C AS DERV_PRTF_CLAS_C, PP4.DERV_PRTF_TYPE_C AS DERV_PRTF_TYPE_C, PP4.VALD_FROM_D AS PRTF_PATY_VALD_FROM_D, PP4.VALD_TO_D AS PRTF_PATY_VALD_TO_D, PP4.EFFT_D AS PRTF_PATY_EFFT_D, PP4.EXPY_D AS PRTF_PATY_EXPY_D, PO4.VALD_FROM_D AS PRTF_OWN_VALD_FROM_D, PO4.VALD_TO_D AS PRTF_OWN_VALD_TO_D, PO4.EFFT_D AS PRTF_OWN_EFFT_D, PO4.EXPY_D AS PRTF_OWN_EXPY_D, PP4.PTCL_N AS PTCL_N, PP4.REL_MNGE_I AS REL_MNGE_I, PP4.PRTF_CODE_X AS PRTF_CODE_X, PO4.DERV_PRTF_ROLE_C AS DERV_PRTF_ROLE_C, PO4.ROLE_PLAY_TYPE_X AS ROLE_PLAY_TYPE_X, PO4.ROLE_PLAY_I AS ROLE_PLAY_I, PP4.SRCE_SYST_C AS SRCE_SYST_C, PP4.ROW_SECU_ACCS_C AS ROW_SECU_ACCS_C FROM %%VTECH%%.DERV_PRTF_PATY_REL AS PP4 INNER JOIN %%VTECH%%.DERV_PRTF_OWN_REL AS PO4 ON PO4.INT_GRUP_I = PP4.INT_GRUP_I AND PO4.DERV_PRTF_TYPE_C = PP4.DERV_PRTF_TYPE_C
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%stardatadb%%"."derv_prtf_paty_own_rel" (
  "paty_i",
  "int_grup_i",
  "derv_prtf_catg_c",
  "derv_prtf_clas_c",
  "derv_prtf_type_c",
  "prtf_paty_vald_from_d",
  "prtf_paty_vald_to_d",
  "prtf_paty_efft_d",
  "prtf_paty_expy_d",
  "prtf_own_vald_from_d",
  "prtf_own_vald_to_d",
  "prtf_own_efft_d",
  "prtf_own_expy_d",
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
  "pp4"."paty_i" AS "paty_i",
  "pp4"."int_grup_i" AS "int_grup_i",
  "pp4"."derv_prtf_catg_c" AS "derv_prtf_catg_c",
  "pp4"."derv_prtf_clas_c" AS "derv_prtf_clas_c",
  "pp4"."derv_prtf_type_c" AS "derv_prtf_type_c",
  "pp4"."vald_from_d" AS "prtf_paty_vald_from_d",
  "pp4"."vald_to_d" AS "prtf_paty_vald_to_d",
  "pp4"."efft_d" AS "prtf_paty_efft_d",
  "pp4"."expy_d" AS "prtf_paty_expy_d",
  "po4"."vald_from_d" AS "prtf_own_vald_from_d",
  "po4"."vald_to_d" AS "prtf_own_vald_to_d",
  "po4"."efft_d" AS "prtf_own_efft_d",
  "po4"."expy_d" AS "prtf_own_expy_d",
  "pp4"."ptcl_n" AS "ptcl_n",
  "pp4"."rel_mnge_i" AS "rel_mnge_i",
  "pp4"."prtf_code_x" AS "prtf_code_x",
  "po4"."derv_prtf_role_c" AS "derv_prtf_role_c",
  "po4"."role_play_type_x" AS "role_play_type_x",
  "po4"."role_play_i" AS "role_play_i",
  "pp4"."srce_syst_c" AS "srce_syst_c",
  "pp4"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "%%vtech%%"."derv_prtf_paty_rel" AS "pp4"
JOIN "%%vtech%%"."derv_prtf_own_rel" AS "po4"
  ON "po4"."derv_prtf_type_c" = "pp4"."derv_prtf_type_c"
  AND "po4"."int_grup_i" = "pp4"."int_grup_i"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_OWN_REL, DERV_PRTF_PATY_OWN_REL, DERV_PRTF_PATY_REL
- **Columns**: DERV_PRTF_CATG_C, DERV_PRTF_CLAS_C, DERV_PRTF_ROLE_C, DERV_PRTF_TYPE_C, EFFT_D, EXPY_D, INT_GRUP_I, PATY_I, PRTF_CODE_X, PTCL_N, REL_MNGE_I, ROLE_PLAY_I, ROLE_PLAY_TYPE_X, ROW_SECU_ACCS_C, SRCE_SYST_C, VALD_FROM_D, VALD_TO_D
- **Functions**: PO4.INT_GRUP_I = PP4.INT_GRUP_I

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
