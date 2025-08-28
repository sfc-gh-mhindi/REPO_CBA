# prtf_tech_paty_rel_psst.sql - BTEQ Analysis

## File Overview
- **File Name**: prtf_tech_paty_rel_psst.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 11
- **SQL Blocks**: 2

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 2 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 33 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 37 | COLLECT_STATS | ` COLLECT STATS  %%STARDATADB%%.DERV_PRTF_PATY_REL;` |
| 39 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 110 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 112 | COLLECT_STATS | `COLLECT STATS  %%STARDATADB%%.DERV_PRTF_PATY_REL;` |
| 114 | IF_ERRORCODE | `.IF ERRORCODE <> 0    THEN .GOTO EXITERR` |
| 118 | LOGOFF | `.LOGOFF` |
| 120 | LABEL | `.LABEL EXITERR` |
| 122 | LOGOFF | `.LOGOFF` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 31-32)
- **Complexity Score**: 6
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Delete from %%STARDATADB%%.DERV_PRTF_PATY_REL All
;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%STARDATADB%%.DERV_PRTF_PATY_REL AS All
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%stardatadb%%"."derv_prtf_paty_rel" AS "all"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_PATY_REL

### SQL Block 2 (Lines 41-109)
- **Complexity Score**: 230
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
Insert into %%STARDATADB%%.DERV_PRTF_PATY_REL
(
       PATY_I 
      ,INT_GRUP_I 
      ,DERV_PRTF_CATG_C 
      ,DERV_PRTF_CLAS_C 
      ,DERV_PRTF_TYPE_C 
      ,VALD_FROM_D 
      ,VALD_TO_D 
      ,EFFT_D
      ,EXPY_D 
      ,PTCL_N
      ,REL_MNGE_I 
      ,PRTF_CODE_X 
      ,SRCE_SYST_C 
      ,ROW_SECU_ACCS_C
)
Select		
   DT1.PATY_I          
  ,DT1.INT_GRUP_I            
  ,GPTE2.PRTF_CATG_C     AS DERV_PRTF_CATG_C
  ,GPTE2.PRTF_CLAS_C     AS DERV_PRTF_CLAS_C  
  ,DT1.DERV_PRTF_TYPE_C
  ,DT1.VALD_FROM_D
  ,DT1.VALD_TO_D
  ,DT1.EFFT_D         
  ,DT1.EXPY_D  
  ,DT1.PTCL_N 
  ,DT1.REL_MNGE_I
  ,DT1.PRTF_CODE_X		
  ,DT1.SRCE_SYST_C
  ,DT1.ROW_SECU_ACCS_C  
From		
  (	
    Select	
       PIG3.PATY_I                                  AS PATY_I		
      ,IG3.INT_GRUP_I                               AS INT_GRUP_I		
      ,PIG3.EFFT_D                                  AS EFFT_D		
      ,PIG3.EXPY_D                                  AS EXPY_D		
      ,IG3.INT_GRUP_TYPE_C                          AS DERV_PRTF_TYPE_C		
      ,CAST(IG3.PTCL_N AS SMALLINT)                 AS PTCL_N		
      ,IG3.REL_MNGE_I                               AS REL_MNGE_I		
      ,(CASE		
          WHEN (IG3.PTCL_N IS NULL) OR (IG3.REL_MNGE_I IS NULL) THEN 'NA'		
          ELSE TRIM(IG3.PTCL_N) || TRIM(IG3.REL_MNGE_I)		
        END)                                        AS PRTF_CODE_X		
      ,PIG3.SRCE_SYST_C                             AS SRCE_SYST_C		
      ,PIG3.ROW_SECU_ACCS_C                         AS ROW_SECU_ACCS_C		
      ,(CASE	
          WHEN IG3.JOIN_FROM_D > PIG3.JOIN_FROM_D THEN IG3.JOIN_FROM_D
          ELSE PIG3.JOIN_FROM_D
        END) as VALD_FROM_D
      ,(CASE	
          WHEN IG3.JOIN_TO_D < PIG3.JOIN_TO_D Then IG3.JOIN_TO_D
          ELSE PIG3.JOIN_TO_D
        END ) as VALD_TO_D
    FROM	
      %%VTECH%%.DERV_PRTF_PATY_PSST PIG3	
      INNER JOIN %%VTECH%%.DERV_PRTF_INT_PSST IG3	
      ON IG3.INT_GRUP_I = PIG3.INT_GRUP_I	
      AND PIG3.JOIN_TO_D >= IG3.JOIN_FROM_D	
      AND PIG3.JOIN_FROM_D <= IG3.JOIN_TO_D	
  ) DT1	

  INNER JOIN %%VTECH%%.GRD_PRTF_TYPE_ENHC_HIST_PSST GPTE2	
  ON GPTE2.PRTF_TYPE_C = DT1.DERV_PRTF_TYPE_C	
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%STARDATADB%%.DERV_PRTF_PATY_REL (PATY_I, INT_GRUP_I, DERV_PRTF_CATG_C, DERV_PRTF_CLAS_C, DERV_PRTF_TYPE_C, VALD_FROM_D, VALD_TO_D, EFFT_D, EXPY_D, PTCL_N, REL_MNGE_I, PRTF_CODE_X, SRCE_SYST_C, ROW_SECU_ACCS_C) SELECT DT1.PATY_I, DT1.INT_GRUP_I, GPTE2.PRTF_CATG_C AS DERV_PRTF_CATG_C, GPTE2.PRTF_CLAS_C AS DERV_PRTF_CLAS_C, DT1.DERV_PRTF_TYPE_C, DT1.VALD_FROM_D, DT1.VALD_TO_D, DT1.EFFT_D, DT1.EXPY_D, DT1.PTCL_N, DT1.REL_MNGE_I, DT1.PRTF_CODE_X, DT1.SRCE_SYST_C, DT1.ROW_SECU_ACCS_C FROM (SELECT PIG3.PATY_I AS PATY_I, IG3.INT_GRUP_I AS INT_GRUP_I, PIG3.EFFT_D AS EFFT_D, PIG3.EXPY_D AS EXPY_D, IG3.INT_GRUP_TYPE_C AS DERV_PRTF_TYPE_C, CAST(IG3.PTCL_N AS SMALLINT) AS PTCL_N, IG3.REL_MNGE_I AS REL_MNGE_I, (CASE WHEN (IG3.PTCL_N IS NULL) OR (IG3.REL_MNGE_I IS NULL) THEN 'NA' ELSE TRIM(IG3.PTCL_N) || TRIM(IG3.REL_MNGE_I) END) AS PRTF_CODE_X, PIG3.SRCE_SYST_C AS SRCE_SYST_C, PIG3.ROW_SECU_ACCS_C AS ROW_SECU_ACCS_C, (CASE WHEN IG3.JOIN_FROM_D > PIG3.JOIN_FROM_D THEN IG3.JOIN_FROM_D ELSE PIG3.JOIN_FROM_D END) AS VALD_FROM_D, (CASE WHEN IG3.JOIN_TO_D < PIG3.JOIN_TO_D THEN IG3.JOIN_TO_D ELSE PIG3.JOIN_TO_D END) AS VALD_TO_D FROM %%VTECH%%.DERV_PRTF_PATY_PSST AS PIG3 INNER JOIN %%VTECH%%.DERV_PRTF_INT_PSST AS IG3 ON IG3.INT_GRUP_I = PIG3.INT_GRUP_I AND PIG3.JOIN_TO_D >= IG3.JOIN_FROM_D AND PIG3.JOIN_FROM_D <= IG3.JOIN_TO_D) AS DT1 INNER JOIN %%VTECH%%.GRD_PRTF_TYPE_ENHC_HIST_PSST AS GPTE2 ON GPTE2.PRTF_TYPE_C = DT1.DERV_PRTF_TYPE_C
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%stardatadb%%"."derv_prtf_paty_rel" (
  "paty_i",
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
  "srce_syst_c",
  "row_secu_accs_c"
)
SELECT
  "pig3"."paty_i" AS "paty_i",
  "ig3"."int_grup_i" AS "int_grup_i",
  "gpte2"."prtf_catg_c" AS "derv_prtf_catg_c",
  "gpte2"."prtf_clas_c" AS "derv_prtf_clas_c",
  "ig3"."int_grup_type_c" AS "derv_prtf_type_c",
  CASE
    WHEN "ig3"."join_from_d" > "pig3"."join_from_d"
    THEN "ig3"."join_from_d"
    ELSE "pig3"."join_from_d"
  END AS "vald_from_d",
  CASE
    WHEN "ig3"."join_to_d" < "pig3"."join_to_d"
    THEN "ig3"."join_to_d"
    ELSE "pig3"."join_to_d"
  END AS "vald_to_d",
  "pig3"."efft_d" AS "efft_d",
  "pig3"."expy_d" AS "expy_d",
  CAST("ig3"."ptcl_n" AS SMALLINT) AS "ptcl_n",
  "ig3"."rel_mnge_i" AS "rel_mnge_i",
  CASE
    WHEN "ig3"."ptcl_n" IS NULL OR "ig3"."rel_mnge_i" IS NULL
    THEN 'NA'
    ELSE TRIM("ig3"."ptcl_n") || TRIM("ig3"."rel_mnge_i")
  END AS "prtf_code_x",
  "pig3"."srce_syst_c" AS "srce_syst_c",
  "pig3"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "%%vtech%%"."derv_prtf_paty_psst" AS "pig3"
JOIN "%%vtech%%"."derv_prtf_int_psst" AS "ig3"
  ON "ig3"."int_grup_i" = "pig3"."int_grup_i"
  AND "ig3"."join_from_d" <= "pig3"."join_to_d"
  AND "ig3"."join_to_d" >= "pig3"."join_from_d"
JOIN "%%vtech%%"."grd_prtf_type_enhc_hist_psst" AS "gpte2"
  ON "gpte2"."prtf_type_c" = "ig3"."int_grup_type_c"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_INT_PSST, DERV_PRTF_PATY_PSST, DERV_PRTF_PATY_REL, GRD_PRTF_TYPE_ENHC_HIST_PSST
- **Columns**: DERV_PRTF_TYPE_C, EFFT_D, EXPY_D, INT_GRUP_I, INT_GRUP_TYPE_C, JOIN_FROM_D, JOIN_TO_D, PATY_I, PRTF_CATG_C, PRTF_CLAS_C, PRTF_CODE_X, PRTF_TYPE_C, PTCL_N, REL_MNGE_I, ROW_SECU_ACCS_C, SRCE_SYST_C, VALD_FROM_D, VALD_TO_D
- **Functions**: (IG3.PTCL_N IS NULL), (IG3.PTCL_N IS NULL) OR (IG3.REL_MNGE_I IS NULL), IG3.INT_GRUP_I = PIG3.INT_GRUP_I, IG3.INT_GRUP_I = PIG3.INT_GRUP_I AND PIG3.JOIN_TO_D >= IG3.JOIN_FROM_D, IG3.JOIN_FROM_D > PIG3.JOIN_FROM_D, IG3.JOIN_TO_D < PIG3.JOIN_TO_D, IG3.PTCL_N, IG3.REL_MNGE_I, None

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
