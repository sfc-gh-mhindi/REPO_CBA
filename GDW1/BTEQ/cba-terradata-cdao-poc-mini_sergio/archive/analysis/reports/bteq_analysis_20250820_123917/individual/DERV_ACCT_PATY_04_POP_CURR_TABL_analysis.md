# DERV_ACCT_PATY_04_POP_CURR_TABL.sql - BTEQ Analysis

## File Overview
- **File Name**: DERV_ACCT_PATY_04_POP_CURR_TABL.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 91
- **SQL Blocks**: 21

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 49 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 51 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.ACCT_PATY_DEDUP;` |
| 52 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 54 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 79 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 82 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.ACCT_PATY_DEDUP;` |
| 83 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 89 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 91 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR;` |
| 92 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 94 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 112 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 116 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR;` |
| 117 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 129 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 261 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 265 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR;` |
| 266 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 275 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 347 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 351 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR;` |
| 352 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 362 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 437 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 439 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR;` |
| 440 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 444 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 496 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 500 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR;` |
| 501 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 505 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 556 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 560 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR;` |
| 561 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 566 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 568 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_WSS;` |
| 569 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 571 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 620 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 623 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_WSS;` |
| 624 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 631 | IMPORT | ` .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.tx...` |
| 654 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 657 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.ACCT_REL_WSS_DITPS;` |
| 658 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 667 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 669 | COLLECT_STATS | ` COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_WSS;` |
| 670 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 675 | IMPORT | ` .IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.tx...` |
| 761 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 763 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_WSS;` |
| 764 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 779 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 781 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR;` |
| 782 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 793 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 795 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_THA;` |
| 796 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 798 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 819 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 821 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_THA;` |
| 822 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 825 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 827 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE;` |
| 828 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 855 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 857 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE;` |
| 858 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 877 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 881 | COLLECT_STATS | ` COLLECT STATS %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE;` |
| 882 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 885 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 887 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_THA;` |
| 888 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 900 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 904 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.ACCT_PATY_REL_THA;` |
| 905 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 907 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 958 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 961 | COLLECT_STATS | ` COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR;` |
| 962 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 973 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 1058 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 1061 | COLLECT_STATS | ` COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR;` |
| 1062 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 1074 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 1242 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 1244 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_CURR;` |
| 1245 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 1253 | LABEL | `.LABEL EXITERR` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 48-48)
- **Complexity Score**: 4
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
DELETE FROM %%DDSTG%%.ACCT_PATY_DEDUP;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%DDSTG%%.ACCT_PATY_DEDUP
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%ddstg%%"."acct_paty_dedup"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_PATY_DEDUP

### SQL Block 2 (Lines 57-78)
- **Complexity Score**: 76
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 3

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.ACCT_PATY_DEDUP
SEL AP.ACCT_I
,PATY_I
,AP.ACCT_I AS ASSC_ACCT_I
,PATY_ACCT_REL_C
,'N'
,SRCE_SYST_C
,EFFT_D
,CASE
        WHEN EFFT_D = EXPY_D THEN EXPY_D
        WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
        ELSE EXPY_D
  END AS EXPY_D      
,AP.ROW_SECU_ACCS_C

FROM %%VTECH%%.ACCT_PATY AP

WHERE :EXTR_D BETWEEN AP.EFFT_D AND AP.EXPY_D  
QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCT_I, PATY_I, PATY_ACCT_REL_C ORDER BY EFFT_D) = 1

;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.ACCT_PATY_DEDUP SELECT AP.ACCT_I, PATY_I, AP.ACCT_I AS ASSC_ACCT_I, PATY_ACCT_REL_C, 'N', SRCE_SYST_C, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D, AP.ROW_SECU_ACCS_C FROM %%VTECH%%.ACCT_PATY AS AP WHERE :EXTR_D BETWEEN AP.EFFT_D AND AP.EXPY_D QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCT_I, PATY_I, PATY_ACCT_REL_C ORDER BY EFFT_D NULLS FIRST) = 1
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- QUALIFY clause
- ROW_NUMBER() OVER
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."acct_paty_dedup"
SELECT
  "ap"."acct_i" AS "acct_i",
  "ap"."paty_i" AS "paty_i",
  "ap"."acct_i" AS "assc_acct_i",
  "ap"."paty_acct_rel_c" AS "paty_acct_rel_c",
  'N' AS "N",
  "ap"."srce_syst_c" AS "srce_syst_c",
  "ap"."efft_d" AS "efft_d",
  CASE
    WHEN "ap"."efft_d" = "ap"."expy_d"
    THEN "ap"."expy_d"
    WHEN "ap"."expy_d" >= :EXTR_D
    THEN CAST('9999-12-31' AS DATE)
    ELSE "ap"."expy_d"
  END AS "expy_d",
  "ap"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "%%vtech%%"."acct_paty" AS "ap"
WHERE
  "ap"."efft_d" <= :EXTR_D AND "ap"."expy_d" >= :EXTR_D
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY "ap"."acct_i", "ap"."paty_i", "ap"."paty_acct_rel_c"
    ORDER BY "ap"."efft_d" NULLS FIRST
  ) = 1
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_PATY, ACCT_PATY_DEDUP
- **Columns**: ACCT_I, EFFT_D, EXPY_D, PATY_ACCT_REL_C, PATY_I, ROW_SECU_ACCS_C, SRCE_SYST_C
- **Functions**: '9999-12-31', EFFT_D = EXPY_D, EXPY_D >= :EXTR_D, None
- **Window_Functions**: None

### SQL Block 3 (Lines 88-88)
- **Complexity Score**: 4
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
DELETE FROM %%DDSTG%%.DERV_ACCT_PATY_CURR;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%DDSTG%%.DERV_ACCT_PATY_CURR
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%ddstg%%"."derv_acct_paty_curr"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_ACCT_PATY_CURR

### SQL Block 4 (Lines 97-111)
- **Complexity Score**: 27
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR
SEL ACCT_I
,PATY_I
,ASSC_ACCT_I
,PATY_ACCT_REL_C
,PRFR_PATY_F
,SRCE_SYST_C
,EFFT_D
,EXPY_D      
,ROW_SECU_ACCS_C

FROM %%DDSTG%%.ACCT_PATY_DEDUP

;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR SELECT ACCT_I, PATY_I, ASSC_ACCT_I, PATY_ACCT_REL_C, PRFR_PATY_F, SRCE_SYST_C, EFFT_D, EXPY_D, ROW_SECU_ACCS_C FROM %%DDSTG%%.ACCT_PATY_DEDUP
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."derv_acct_paty_curr"
SELECT
  "acct_paty_dedup"."acct_i" AS "acct_i",
  "acct_paty_dedup"."paty_i" AS "paty_i",
  "acct_paty_dedup"."assc_acct_i" AS "assc_acct_i",
  "acct_paty_dedup"."paty_acct_rel_c" AS "paty_acct_rel_c",
  "acct_paty_dedup"."prfr_paty_f" AS "prfr_paty_f",
  "acct_paty_dedup"."srce_syst_c" AS "srce_syst_c",
  "acct_paty_dedup"."efft_d" AS "efft_d",
  "acct_paty_dedup"."expy_d" AS "expy_d",
  "acct_paty_dedup"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "%%ddstg%%"."acct_paty_dedup" AS "acct_paty_dedup"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_PATY_DEDUP, DERV_ACCT_PATY_CURR
- **Columns**: ACCT_I, ASSC_ACCT_I, EFFT_D, EXPY_D, PATY_ACCT_REL_C, PATY_I, PRFR_PATY_F, ROW_SECU_ACCS_C, SRCE_SYST_C

### SQL Block 5 (Lines 132-260)
- **Complexity Score**: 543
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR
SELECT AX.BPS_ACCT_I AS ACCT_I
             ,AP.PATY_I
            ,CBS_ACCT_I AS ASSC_ACCT_I
            ,AP.PATY_ACCT_REL_C
            ,'N' AS PRFR_PATY_F  
            ,AP.SRCE_SYST_C
          ,(CASE WHEN AP.EFFT_D > AX.EFFT_D THEN AP.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D
          ,(CASE WHEN AP.EXPY_D < AX.EXPY_D THEN AP.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D
          ,AP.ROW_SECU_ACCS_C

 FROM (
                 SELECT ACCT_I
                               ,PATY_I
                               ,PATY_ACCT_REL_C
                               ,SRCE_SYST_C
                               ,EFFT_D
                               ,CASE
                                       WHEN EFFT_D = EXPY_D THEN EXPY_D
                                       WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                                      ELSE EXPY_D
                                END AS EXPY_D      
                             ,ROW_SECU_ACCS_C
                 FROM   %%DDSTG%%.ACCT_PATY_DEDUP  
              WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
          ) AP   
  JOIN (
              SELECT CBS_ACCT_I
                           ,BPS_ACCT_I
                           ,EFFT_D
                           ,CASE
                                       WHEN EFFT_D = EXPY_D THEN EXPY_D
                                       WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                                      ELSE EXPY_D
                            END AS EXPY_D    
                FROM   %%VTECH%%.ACCT_XREF_BPS_CBS
              WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
          )   AX 
ON AP.ACCT_I = AX.CBS_ACCT_I 
 WHERE (
                     (AP.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D) 
                OR (AX.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
             )
 
  
GROUP BY 1,2,3,4,5,6,7,8,9
              
UNION ALL

SELECT
  AR.OBJC_ACCT_I AS ACCT_I
, BPS.PATY_I
, BPS.CBS_ACCT_I AS ASSC_ACCT_I
, BPS.PATY_ACCT_REL_C
,'N' AS PRFR_PATY_F  
, BPS.SRCE_SYST_C
, (CASE WHEN AR.EFFT_D >  BPS.EFFT_D  THEN AR.EFFT_D  ELSE BPS.EFFT_D END) AS EFFT_D
, (CASE WHEN AR.EXPY_D <  BPS.EXPY_D THEN AR.EXPY_D ELSE BPS.EXPY_D END) AS EXPY_D
 ,BPS.ROW_SECU_ACCS_C

FROM  (SEL SUBJ_ACCT_I
                      ,OBJC_ACCT_I
                      ,EFFT_D
                      ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                                 ELSE EXPY_D
                        END AS EXPY_D    
             FROM   %%VTECH%%.ACCT_REL  
           WHERE REL_C = 'FLBLL'
                AND :EXTR_D BETWEEN EFFT_D AND EXPY_D ) AR
JOIN

(
SELECT AX.BPS_ACCT_I
            ,CBS_ACCT_I 
            ,AP.PATY_I
            ,AP.PATY_ACCT_REL_C
            ,AP.SRCE_SYST_C
          ,(CASE WHEN AP.EFFT_D > AX.EFFT_D THEN AP.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D
          ,(CASE WHEN AP.EXPY_D < AX.EXPY_D THEN AP.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D
           ,AP.ROW_SECU_ACCS_C
 FROM (
                SEL ACCT_I
                     ,PATY_I
                     ,PATY_ACCT_REL_C
                     ,SRCE_SYST_C
                     ,EFFT_D
                     ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                                 ELSE EXPY_D
                        END AS EXPY_D
                     ,ROW_SECU_ACCS_C       
           FROM    %%DDSTG%%.ACCT_PATY_DEDUP
             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP 

JOIN  (
              SELECT CBS_ACCT_I
                           ,BPS_ACCT_I
                           ,EFFT_D
                           ,CASE
                                      WHEN EFFT_D = EXPY_D THEN EXPY_D
                                       WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                                      ELSE EXPY_D
                            END AS EXPY_D    
                FROM   %%VTECH%%.ACCT_XREF_BPS_CBS
              WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
          )   AX 
ON AP.ACCT_I = AX.CBS_ACCT_I 
WHERE  (
                     (AP.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D) 
                OR (AX.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
                   )


              
        GROUP BY 1, 2, 3, 4, 5,6,7,8
    
) BPS
ON AR.SUBJ_ACCT_I = BPS.BPS_ACCT_I 

WHERE (
                (AR.EFFT_D BETWEEN BPS.EFFT_D AND BPS.EXPY_D) OR 
                (BPS.EFFT_D BETWEEN AR.EFFT_D AND AR.EXPY_D)
              )
GROUP BY 1,2,3,4,5,6,7,8,9

;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR SELECT AX.BPS_ACCT_I AS ACCT_I, AP.PATY_I, CBS_ACCT_I AS ASSC_ACCT_I, AP.PATY_ACCT_REL_C, 'N' AS PRFR_PATY_F, AP.SRCE_SYST_C, (CASE WHEN AP.EFFT_D > AX.EFFT_D THEN AP.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D, (CASE WHEN AP.EXPY_D < AX.EXPY_D THEN AP.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D, AP.ROW_SECU_ACCS_C FROM (SELECT ACCT_I, PATY_I, PATY_ACCT_REL_C, SRCE_SYST_C, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D, ROW_SECU_ACCS_C FROM %%DDSTG%%.ACCT_PATY_DEDUP WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS AP JOIN (SELECT CBS_ACCT_I, BPS_ACCT_I, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D FROM %%VTECH%%.ACCT_XREF_BPS_CBS WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS AX ON AP.ACCT_I = AX.CBS_ACCT_I WHERE ((AP.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D) OR (AX.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)) GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9 UNION ALL SELECT AR.OBJC_ACCT_I AS ACCT_I, BPS.PATY_I, BPS.CBS_ACCT_I AS ASSC_ACCT_I, BPS.PATY_ACCT_REL_C, 'N' AS PRFR_PATY_F, BPS.SRCE_SYST_C, (CASE WHEN AR.EFFT_D > BPS.EFFT_D THEN AR.EFFT_D ELSE BPS.EFFT_D END) AS EFFT_D, (CASE WHEN AR.EXPY_D < BPS.EXPY_D THEN AR.EXPY_D ELSE BPS.EXPY_D END) AS EXPY_D, BPS.ROW_SECU_ACCS_C FROM (SELECT SUBJ_ACCT_I, OBJC_ACCT_I, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D FROM %%VTECH%%.ACCT_REL WHERE REL_C = 'FLBLL' AND :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS AR JOIN (SELECT AX.BPS_ACCT_I, CBS_ACCT_I, AP.PATY_I, AP.PATY_ACCT_REL_C, AP.SRCE_SYST_C, (CASE WHEN AP.EFFT_D > AX.EFFT_D THEN AP.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D, (CASE WHEN AP.EXPY_D < AX.EXPY_D THEN AP.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D, AP.ROW_SECU_ACCS_C FROM (SELECT ACCT_I, PATY_I, PATY_ACCT_REL_C, SRCE_SYST_C, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D, ROW_SECU_ACCS_C FROM %%DDSTG%%.ACCT_PATY_DEDUP WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS AP JOIN (SELECT CBS_ACCT_I, BPS_ACCT_I, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D FROM %%VTECH%%.ACCT_XREF_BPS_CBS WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS AX ON AP.ACCT_I = AX.CBS_ACCT_I WHERE ((AP.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D) OR (AX.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)) GROUP BY 1, 2, 3, 4, 5, 6, 7, 8) AS BPS ON AR.SUBJ_ACCT_I = BPS.BPS_ACCT_I WHERE ((AR.EFFT_D BETWEEN BPS.EFFT_D AND BPS.EXPY_D) OR (BPS.EFFT_D BETWEEN AR.EFFT_D AND AR.EXPY_D)) GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."derv_acct_paty_curr"
WITH "ap" AS (
  SELECT
    "acct_paty_dedup"."acct_i" AS "acct_i",
    "acct_paty_dedup"."paty_i" AS "paty_i",
    "acct_paty_dedup"."paty_acct_rel_c" AS "paty_acct_rel_c",
    "acct_paty_dedup"."srce_syst_c" AS "srce_syst_c",
    "acct_paty_dedup"."efft_d" AS "efft_d",
    CASE
      WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
      THEN "acct_paty_dedup"."expy_d"
      WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "acct_paty_dedup"."expy_d"
    END AS "expy_d",
    "acct_paty_dedup"."row_secu_accs_c" AS "row_secu_accs_c"
  FROM "%%ddstg%%"."acct_paty_dedup" AS "acct_paty_dedup"
  WHERE
    "acct_paty_dedup"."efft_d" <= :EXTR_D
    AND CASE
      WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
      THEN "acct_paty_dedup"."expy_d"
      WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "acct_paty_dedup"."expy_d"
    END >= :EXTR_D
), "ax" AS (
  SELECT
    "acct_xref_bps_cbs"."cbs_acct_i" AS "cbs_acct_i",
    "acct_xref_bps_cbs"."bps_acct_i" AS "bps_acct_i",
    "acct_xref_bps_cbs"."efft_d" AS "efft_d",
    CASE
      WHEN "acct_xref_bps_cbs"."efft_d" = "acct_xref_bps_cbs"."expy_d"
      THEN "acct_xref_bps_cbs"."expy_d"
      WHEN "acct_xref_bps_cbs"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "acct_xref_bps_cbs"."expy_d"
    END AS "expy_d"
  FROM "%%vtech%%"."acct_xref_bps_cbs" AS "acct_xref_bps_cbs"
  WHERE
    "acct_xref_bps_cbs"."efft_d" <= :EXTR_D
    AND CASE
      WHEN "acct_xref_bps_cbs"."efft_d" = "acct_xref_bps_cbs"."expy_d"
      THEN "acct_xref_bps_cbs"."expy_d"
      WHEN "acct_xref_bps_cbs"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "acct_xref_bps_cbs"."expy_d"
    END >= :EXTR_D
), "bps" AS (
  SELECT
    "ax"."bps_acct_i" AS "bps_acct_i",
    "ax"."cbs_acct_i" AS "cbs_acct_i",
    "ap"."paty_i" AS "paty_i",
    "ap"."paty_acct_rel_c" AS "paty_acct_rel_c",
    "ap"."srce_syst_c" AS "srce_syst_c",
    (
      CASE WHEN "ap"."efft_d" > "ax"."efft_d" THEN "ap"."efft_d" ELSE "ax"."efft_d" END
    ) AS "efft_d",
    (
      CASE WHEN "ap"."expy_d" < "ax"."expy_d" THEN "ap"."expy_d" ELSE "ax"."expy_d" END
    ) AS "expy_d",
    "ap"."row_secu_accs_c" AS "row_secu_accs_c"
  FROM "ap" AS "ap"
  JOIN "ax" AS "ax"
    ON "ap"."acct_i" = "ax"."cbs_acct_i"
    AND (
      (
        "ap"."efft_d" <= "ax"."efft_d" AND "ap"."expy_d" >= "ax"."efft_d"
      )
      OR (
        "ap"."efft_d" <= "ax"."expy_d" AND "ap"."efft_d" >= "ax"."efft_d"
      )
    )
  WHERE
    (
      "ap"."efft_d" <= "ax"."efft_d" AND "ap"."expy_d" >= "ax"."efft_d"
    )
    OR (
      "ap"."efft_d" <= "ax"."expy_d" AND "ap"."efft_d" >= "ax"."efft_d"
    )
  GROUP BY
    "ax"."bps_acct_i",
    "ax"."cbs_acct_i",
    "ap"."paty_i",
    "ap"."paty_acct_rel_c",
    "ap"."srce_syst_c",
    (
      CASE WHEN "ap"."efft_d" > "ax"."efft_d" THEN "ap"."efft_d" ELSE "ax"."efft_d" END
    ),
    (
      CASE WHEN "ap"."expy_d" < "ax"."expy_d" THEN "ap"."expy_d" ELSE "ax"."expy_d" END
    ),
    "ap"."row_secu_accs_c"
)
SELECT
  "ax"."bps_acct_i" AS "acct_i",
  "ap"."paty_i" AS "paty_i",
  "ax"."cbs_acct_i" AS "assc_acct_i",
  "ap"."paty_acct_rel_c" AS "paty_acct_rel_c",
  'N' AS "prfr_paty_f",
  "ap"."srce_syst_c" AS "srce_syst_c",
  (
    CASE WHEN "ap"."efft_d" > "ax"."efft_d" THEN "ap"."efft_d" ELSE "ax"."efft_d" END
  ) AS "efft_d",
  (
    CASE WHEN "ap"."expy_d" < "ax"."expy_d" THEN "ap"."expy_d" ELSE "ax"."expy_d" END
  ) AS "expy_d",
  "ap"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "ap" AS "ap"
JOIN "ax" AS "ax"
  ON "ap"."acct_i" = "ax"."cbs_acct_i"
  AND (
    (
      "ap"."efft_d" <= "ax"."efft_d" AND "ap"."expy_d" >= "ax"."efft_d"
    )
    OR (
      "ap"."efft_d" <= "ax"."expy_d" AND "ap"."efft_d" >= "ax"."efft_d"
    )
  )
WHERE
  (
    "ap"."efft_d" <= "ax"."efft_d" AND "ap"."expy_d" >= "ax"."efft_d"
  )
  OR (
    "ap"."efft_d" <= "ax"."expy_d" AND "ap"."efft_d" >= "ax"."efft_d"
  )
GROUP BY
  "ax"."bps_acct_i",
  "ap"."paty_i",
  "ax"."cbs_acct_i",
  "ap"."paty_acct_rel_c",
  5,
  "ap"."srce_syst_c",
  (
    CASE WHEN "ap"."efft_d" > "ax"."efft_d" THEN "ap"."efft_d" ELSE "ax"."efft_d" END
  ),
  (
    CASE WHEN "ap"."expy_d" < "ax"."expy_d" THEN "ap"."expy_d" ELSE "ax"."expy_d" END
  ),
  "ap"."row_secu_accs_c"
UNION ALL
SELECT
  "acct_rel"."objc_acct_i" AS "acct_i",
  "bps"."paty_i" AS "paty_i",
  "bps"."cbs_acct_i" AS "assc_acct_i",
  "bps"."paty_acct_rel_c" AS "paty_acct_rel_c",
  'N' AS "prfr_paty_f",
  "bps"."srce_syst_c" AS "srce_syst_c",
  (
    CASE
      WHEN "acct_rel"."efft_d" > "bps"."efft_d"
      THEN "acct_rel"."efft_d"
      ELSE "bps"."efft_d"
    END
  ) AS "efft_d",
  (
    CASE
      WHEN CASE
        WHEN "acct_rel"."efft_d" = "acct_rel"."expy_d"
        THEN "acct_rel"."expy_d"
        WHEN "acct_rel"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_rel"."expy_d"
      END < "bps"."expy_d"
      THEN CASE
        WHEN "acct_rel"."efft_d" = "acct_rel"."expy_d"
        THEN "acct_rel"."expy_d"
        WHEN "acct_rel"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_rel"."expy_d"
      END
      ELSE "bps"."expy_d"
    END
  ) AS "expy_d",
  "bps"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "%%vtech%%"."acct_rel" AS "acct_rel"
JOIN "bps" AS "bps"
  ON "acct_rel"."subj_acct_i" = "bps"."bps_acct_i"
  AND (
    (
      "acct_rel"."efft_d" <= "bps"."efft_d"
      AND "bps"."efft_d" <= CASE
        WHEN "acct_rel"."efft_d" = "acct_rel"."expy_d"
        THEN "acct_rel"."expy_d"
        WHEN "acct_rel"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_rel"."expy_d"
      END
    )
    OR (
      "acct_rel"."efft_d" <= "bps"."expy_d" AND "acct_rel"."efft_d" >= "bps"."efft_d"
    )
  )
WHERE
  "acct_rel"."efft_d" <= :EXTR_D
  AND "acct_rel"."rel_c" = 'FLBLL'
  AND (
    (
      "acct_rel"."efft_d" <= "bps"."efft_d"
      AND "bps"."efft_d" <= CASE
        WHEN "acct_rel"."efft_d" = "acct_rel"."expy_d"
        THEN "acct_rel"."expy_d"
        WHEN "acct_rel"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_rel"."expy_d"
      END
    )
    OR (
      "acct_rel"."efft_d" <= "bps"."expy_d" AND "acct_rel"."efft_d" >= "bps"."efft_d"
    )
  )
  AND CASE
    WHEN "acct_rel"."efft_d" = "acct_rel"."expy_d"
    THEN "acct_rel"."expy_d"
    WHEN "acct_rel"."expy_d" >= :EXTR_D
    THEN CAST('9999-12-31' AS DATE)
    ELSE "acct_rel"."expy_d"
  END >= :EXTR_D
GROUP BY
  "acct_rel"."objc_acct_i",
  "bps"."paty_i",
  "bps"."cbs_acct_i",
  "bps"."paty_acct_rel_c",
  5,
  "bps"."srce_syst_c",
  (
    CASE
      WHEN "acct_rel"."efft_d" > "bps"."efft_d"
      THEN "acct_rel"."efft_d"
      ELSE "bps"."efft_d"
    END
  ),
  (
    CASE
      WHEN CASE
        WHEN "acct_rel"."efft_d" = "acct_rel"."expy_d"
        THEN "acct_rel"."expy_d"
        WHEN "acct_rel"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_rel"."expy_d"
      END < "bps"."expy_d"
      THEN CASE
        WHEN "acct_rel"."efft_d" = "acct_rel"."expy_d"
        THEN "acct_rel"."expy_d"
        WHEN "acct_rel"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_rel"."expy_d"
      END
      ELSE "bps"."expy_d"
    END
  ),
  "bps"."row_secu_accs_c"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_PATY_DEDUP, ACCT_REL, ACCT_XREF_BPS_CBS, DERV_ACCT_PATY_CURR
- **Columns**: ACCT_I, BPS_ACCT_I, CBS_ACCT_I, EFFT_D, EXPY_D, OBJC_ACCT_I, PATY_ACCT_REL_C, PATY_I, REL_C, ROW_SECU_ACCS_C, SRCE_SYST_C, SUBJ_ACCT_I
- **Functions**: '9999-12-31', (AP.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D), (AR.EFFT_D BETWEEN BPS.EFFT_D AND BPS.EXPY_D), AP.EFFT_D > AX.EFFT_D, AP.EXPY_D < AX.EXPY_D, AR.EFFT_D > BPS.EFFT_D, AR.EXPY_D < BPS.EXPY_D, EFFT_D = EXPY_D, EXPY_D >= :EXTR_D, None, REL_C = 'FLBLL'

### SQL Block 6 (Lines 278-346)
- **Complexity Score**: 330
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR
 SEL CLS.ACCT_I
      ,AP.PATY_I
     ,CLS.GDW_ACCT_I AS ASSC_ACCT_I 
     ,AP.PATY_ACCT_REL_C
     ,'N' AS PRFR_PATY_F  
     ,AP.SRCE_SYST_C
   , (CASE WHEN AP.EFFT_D > CLS.EFFT_D THEN AP.EFFT_D ELSE CLS.EFFT_D END) AS EFFT_D
   , (CASE WHEN AP.EXPY_D < CLS.EXPY_D THEN AP.EXPY_D ELSE CLS.EXPY_D END) AS EXPY_D
   ,AP.ROW_SECU_ACCS_C
FROM  (
                SEL ACCT_I
                     ,PATY_I
                     ,PATY_ACCT_REL_C
                     ,SRCE_SYST_C
                     ,EFFT_D
                     ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                                 ELSE EXPY_D
                        END AS EXPY_D
                     ,ROW_SECU_ACCS_C       
           FROM    %%DDSTG%%.ACCT_PATY_DEDUP
             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP 

JOIN 
    (SEL CF.ACCT_I
    , 'CLSCO'||TRIM(CUP.CRIS_DEBT_I) AS GDW_ACCT_I
    ,(CASE WHEN CF.EFFT_D > CUP.EFFT_D THEN CF.EFFT_D ELSE CUP.EFFT_D END) AS EFFT_D
    ,(CASE WHEN CF.EXPY_D < CUP.EXPY_D THEN CF.EXPY_D ELSE CUP.EXPY_D END) AS EXPY_D
     FROM  (SEL ACCT_I
                          ,SRCE_SYST_PATY_I
                         ,EFFT_D
                         ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                                 ELSE EXPY_D
                        END AS EXPY_D
             FROM %%VTECH%%.CLS_FCLY
             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  CF

     JOIN  (
                   SEL SRCE_SYST_PATY_I
                         ,CRIS_DEBT_I
                         ,EFFT_D
                         ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                                 ELSE EXPY_D
                        END AS EXPY_D
                  FROM %%VTECH%%.CLS_UNID_PATY
                  WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) CUP 
     ON CUP.SRCE_SYST_PATY_I  = CF.SRCE_SYST_PATY_I 
     WHERE (
            (CUP.EFFT_D BETWEEN CF.EFFT_D AND CF.EXPY_D) 
        OR (CF.EFFT_D BETWEEN CUP.EFFT_D AND CUP.EXPY_D)
       )
 
        GROUP BY 1, 2, 3, 4
) AS CLS   
ON CLS.GDW_ACCT_I = AP.ACCT_I
WHERE  (
        (AP.EFFT_D BETWEEN CLS.EFFT_D AND CLS.EXPY_D) OR 
         (CLS.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
      )


GROUP BY 1,2,3,4,5,6,7,8,9
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR SELECT CLS.ACCT_I, AP.PATY_I, CLS.GDW_ACCT_I AS ASSC_ACCT_I, AP.PATY_ACCT_REL_C, 'N' AS PRFR_PATY_F, AP.SRCE_SYST_C, (CASE WHEN AP.EFFT_D > CLS.EFFT_D THEN AP.EFFT_D ELSE CLS.EFFT_D END) AS EFFT_D, (CASE WHEN AP.EXPY_D < CLS.EXPY_D THEN AP.EXPY_D ELSE CLS.EXPY_D END) AS EXPY_D, AP.ROW_SECU_ACCS_C FROM (SELECT ACCT_I, PATY_I, PATY_ACCT_REL_C, SRCE_SYST_C, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D, ROW_SECU_ACCS_C FROM %%DDSTG%%.ACCT_PATY_DEDUP WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS AP JOIN (SELECT CF.ACCT_I, 'CLSCO' || TRIM(CUP.CRIS_DEBT_I) AS GDW_ACCT_I, (CASE WHEN CF.EFFT_D > CUP.EFFT_D THEN CF.EFFT_D ELSE CUP.EFFT_D END) AS EFFT_D, (CASE WHEN CF.EXPY_D < CUP.EXPY_D THEN CF.EXPY_D ELSE CUP.EXPY_D END) AS EXPY_D FROM (SELECT ACCT_I, SRCE_SYST_PATY_I, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D FROM %%VTECH%%.CLS_FCLY WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS CF JOIN (SELECT SRCE_SYST_PATY_I, CRIS_DEBT_I, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D FROM %%VTECH%%.CLS_UNID_PATY WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS CUP ON CUP.SRCE_SYST_PATY_I = CF.SRCE_SYST_PATY_I WHERE ((CUP.EFFT_D BETWEEN CF.EFFT_D AND CF.EXPY_D) OR (CF.EFFT_D BETWEEN CUP.EFFT_D AND CUP.EXPY_D)) GROUP BY 1, 2, 3, 4) AS CLS ON CLS.GDW_ACCT_I = AP.ACCT_I WHERE ((AP.EFFT_D BETWEEN CLS.EFFT_D AND CLS.EXPY_D) OR (CLS.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)) GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."derv_acct_paty_curr"
WITH "cls" AS (
  SELECT
    "cls_fcly"."acct_i" AS "acct_i",
    'CLSCO' || TRIM("cls_unid_paty"."cris_debt_i") AS "gdw_acct_i",
    (
      CASE
        WHEN "cls_fcly"."efft_d" > "cls_unid_paty"."efft_d"
        THEN "cls_fcly"."efft_d"
        ELSE "cls_unid_paty"."efft_d"
      END
    ) AS "efft_d",
    (
      CASE
        WHEN CASE
          WHEN "cls_fcly"."efft_d" = "cls_fcly"."expy_d"
          THEN "cls_fcly"."expy_d"
          WHEN "cls_fcly"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "cls_fcly"."expy_d"
        END < CASE
          WHEN "cls_unid_paty"."efft_d" = "cls_unid_paty"."expy_d"
          THEN "cls_unid_paty"."expy_d"
          WHEN "cls_unid_paty"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "cls_unid_paty"."expy_d"
        END
        THEN CASE
          WHEN "cls_fcly"."efft_d" = "cls_fcly"."expy_d"
          THEN "cls_fcly"."expy_d"
          WHEN "cls_fcly"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "cls_fcly"."expy_d"
        END
        ELSE CASE
          WHEN "cls_unid_paty"."efft_d" = "cls_unid_paty"."expy_d"
          THEN "cls_unid_paty"."expy_d"
          WHEN "cls_unid_paty"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "cls_unid_paty"."expy_d"
        END
      END
    ) AS "expy_d"
  FROM "%%vtech%%"."cls_fcly" AS "cls_fcly"
  JOIN "%%vtech%%"."cls_unid_paty" AS "cls_unid_paty"
    ON "cls_fcly"."srce_syst_paty_i" = "cls_unid_paty"."srce_syst_paty_i"
    AND "cls_unid_paty"."efft_d" <= :EXTR_D
    AND (
      (
        "cls_fcly"."efft_d" <= "cls_unid_paty"."efft_d"
        AND "cls_unid_paty"."efft_d" <= CASE
          WHEN "cls_fcly"."efft_d" = "cls_fcly"."expy_d"
          THEN "cls_fcly"."expy_d"
          WHEN "cls_fcly"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "cls_fcly"."expy_d"
        END
      )
      OR (
        "cls_fcly"."efft_d" <= CASE
          WHEN "cls_unid_paty"."efft_d" = "cls_unid_paty"."expy_d"
          THEN "cls_unid_paty"."expy_d"
          WHEN "cls_unid_paty"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "cls_unid_paty"."expy_d"
        END
        AND "cls_fcly"."efft_d" >= "cls_unid_paty"."efft_d"
      )
    )
    AND CASE
      WHEN "cls_unid_paty"."efft_d" = "cls_unid_paty"."expy_d"
      THEN "cls_unid_paty"."expy_d"
      WHEN "cls_unid_paty"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "cls_unid_paty"."expy_d"
    END >= :EXTR_D
  WHERE
    "cls_fcly"."efft_d" <= :EXTR_D
    AND (
      (
        "cls_fcly"."efft_d" <= "cls_unid_paty"."efft_d"
        AND "cls_unid_paty"."efft_d" <= CASE
          WHEN "cls_fcly"."efft_d" = "cls_fcly"."expy_d"
          THEN "cls_fcly"."expy_d"
          WHEN "cls_fcly"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "cls_fcly"."expy_d"
        END
      )
      OR (
        "cls_fcly"."efft_d" <= CASE
          WHEN "cls_unid_paty"."efft_d" = "cls_unid_paty"."expy_d"
          THEN "cls_unid_paty"."expy_d"
          WHEN "cls_unid_paty"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "cls_unid_paty"."expy_d"
        END
        AND "cls_fcly"."efft_d" >= "cls_unid_paty"."efft_d"
      )
    )
    AND CASE
      WHEN "cls_fcly"."efft_d" = "cls_fcly"."expy_d"
      THEN "cls_fcly"."expy_d"
      WHEN "cls_fcly"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "cls_fcly"."expy_d"
    END >= :EXTR_D
  GROUP BY
    "cls_fcly"."acct_i",
    'CLSCO' || TRIM("cls_unid_paty"."cris_debt_i"),
    (
      CASE
        WHEN "cls_fcly"."efft_d" > "cls_unid_paty"."efft_d"
        THEN "cls_fcly"."efft_d"
        ELSE "cls_unid_paty"."efft_d"
      END
    ),
    (
      CASE
        WHEN CASE
          WHEN "cls_fcly"."efft_d" = "cls_fcly"."expy_d"
          THEN "cls_fcly"."expy_d"
          WHEN "cls_fcly"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "cls_fcly"."expy_d"
        END < CASE
          WHEN "cls_unid_paty"."efft_d" = "cls_unid_paty"."expy_d"
          THEN "cls_unid_paty"."expy_d"
          WHEN "cls_unid_paty"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "cls_unid_paty"."expy_d"
        END
        THEN CASE
          WHEN "cls_fcly"."efft_d" = "cls_fcly"."expy_d"
          THEN "cls_fcly"."expy_d"
          WHEN "cls_fcly"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "cls_fcly"."expy_d"
        END
        ELSE CASE
          WHEN "cls_unid_paty"."efft_d" = "cls_unid_paty"."expy_d"
          THEN "cls_unid_paty"."expy_d"
          WHEN "cls_unid_paty"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "cls_unid_paty"."expy_d"
        END
      END
    )
)
SELECT
  "cls"."acct_i" AS "acct_i",
  "acct_paty_dedup"."paty_i" AS "paty_i",
  "cls"."gdw_acct_i" AS "assc_acct_i",
  "acct_paty_dedup"."paty_acct_rel_c" AS "paty_acct_rel_c",
  'N' AS "prfr_paty_f",
  "acct_paty_dedup"."srce_syst_c" AS "srce_syst_c",
  (
    CASE
      WHEN "acct_paty_dedup"."efft_d" > "cls"."efft_d"
      THEN "acct_paty_dedup"."efft_d"
      ELSE "cls"."efft_d"
    END
  ) AS "efft_d",
  (
    CASE
      WHEN CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END < "cls"."expy_d"
      THEN CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
      ELSE "cls"."expy_d"
    END
  ) AS "expy_d",
  "acct_paty_dedup"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "%%ddstg%%"."acct_paty_dedup" AS "acct_paty_dedup"
JOIN "cls" AS "cls"
  ON "acct_paty_dedup"."acct_i" = "cls"."gdw_acct_i"
  AND (
    (
      "acct_paty_dedup"."efft_d" <= "cls"."efft_d"
      AND "cls"."efft_d" <= CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
    )
    OR (
      "acct_paty_dedup"."efft_d" <= "cls"."expy_d"
      AND "acct_paty_dedup"."efft_d" >= "cls"."efft_d"
    )
  )
WHERE
  "acct_paty_dedup"."efft_d" <= :EXTR_D
  AND (
    (
      "acct_paty_dedup"."efft_d" <= "cls"."efft_d"
      AND "cls"."efft_d" <= CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
    )
    OR (
      "acct_paty_dedup"."efft_d" <= "cls"."expy_d"
      AND "acct_paty_dedup"."efft_d" >= "cls"."efft_d"
    )
  )
  AND CASE
    WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
    THEN "acct_paty_dedup"."expy_d"
    WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
    THEN CAST('9999-12-31' AS DATE)
    ELSE "acct_paty_dedup"."expy_d"
  END >= :EXTR_D
GROUP BY
  "cls"."acct_i",
  "acct_paty_dedup"."paty_i",
  "cls"."gdw_acct_i",
  "acct_paty_dedup"."paty_acct_rel_c",
  5,
  "acct_paty_dedup"."srce_syst_c",
  (
    CASE
      WHEN "acct_paty_dedup"."efft_d" > "cls"."efft_d"
      THEN "acct_paty_dedup"."efft_d"
      ELSE "cls"."efft_d"
    END
  ),
  (
    CASE
      WHEN CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END < "cls"."expy_d"
      THEN CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
      ELSE "cls"."expy_d"
    END
  ),
  "acct_paty_dedup"."row_secu_accs_c"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_PATY_DEDUP, CLS_FCLY, CLS_UNID_PATY, DERV_ACCT_PATY_CURR
- **Columns**: ACCT_I, CRIS_DEBT_I, EFFT_D, EXPY_D, GDW_ACCT_I, PATY_ACCT_REL_C, PATY_I, ROW_SECU_ACCS_C, SRCE_SYST_C, SRCE_SYST_PATY_I
- **Functions**: '9999-12-31', (AP.EFFT_D BETWEEN CLS.EFFT_D AND CLS.EXPY_D), (CUP.EFFT_D BETWEEN CF.EFFT_D AND CF.EXPY_D), AP.EFFT_D > CLS.EFFT_D, AP.EXPY_D < CLS.EXPY_D, CF.EFFT_D > CUP.EFFT_D, CF.EXPY_D < CUP.EXPY_D, CUP.CRIS_DEBT_I, EFFT_D = EXPY_D, EXPY_D >= :EXTR_D, None

### SQL Block 7 (Lines 365-436)
- **Complexity Score**: 313
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR
SELECT DAR.MERC_ACCT_I AS ACCT_I
           , AP.PATY_I
            ,DAR.MAS_MERC_ACCT_I
           , AP.PATY_ACCT_REL_C
           ,'N' AS PRFR_PATY_I
           , AP.SRCE_SYST_C
          , (CASE WHEN AP.EFFT_D >  DAR.EFFT_D  THEN AP.EFFT_D  ELSE DAR.EFFT_D END) AS EFFT_D
          , (CASE WHEN AP.EXPY_D <  DAR.EXPY_D THEN AP.EXPY_D ELSE DAR.EXPY_D END) AS EXPY_D
           ,AP.ROW_SECU_ACCS_C
FROM   (
                SEL ACCT_I
                     ,PATY_I
                     ,PATY_ACCT_REL_C
                     ,SRCE_SYST_C
                     ,EFFT_D
                     ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                                 ELSE EXPY_D
                        END AS EXPY_D
                     ,ROW_SECU_ACCS_C       
           FROM    %%DDSTG%%.ACCT_PATY_DEDUP
             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP 


JOIN (

SELECT DA.MERC_ACCT_I
             ,AX.MAS_MERC_ACCT_I
              ,(CASE WHEN DA.EFFT_D > AX.EFFT_D THEN DA.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D
               ,(CASE WHEN DA.EXPY_D < AX.EXPY_D THEN DA.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D
FROM 
(SEL  MERC_ACCT_I
         ,EFFT_D
         ,CASE
                 WHEN EFFT_D = EXPY_D THEN EXPY_D
                 WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                    ELSE EXPY_D
       END AS EXPY_D
   FROM %%VTECH%%.DAR_ACCT
   WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) DA
JOIN (
SELECT MAS_MERC_ACCT_I
             ,DAR_ACCT_I
             ,EFFT_D
              ,CASE
                 WHEN EFFT_D = EXPY_D THEN EXPY_D
                 WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                    ELSE EXPY_D
              END AS EXPY_D
     FROM        %%VTECH%%.ACCT_XREF_MAS_DAR
     WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AX
  ON AX.DAR_ACCT_I = DA.MERC_ACCT_I
WHERE (
               (AX.EFFT_D BETWEEN DA.EFFT_D AND DA.EXPY_D) 
         OR (DA.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D)
              )
    
GROUP BY 1, 2,3,4

) AS DAR   
ON DAR.MAS_MERC_ACCT_I = AP.ACCT_I

WHERE (
               (AP.EFFT_D BETWEEN DAR.EFFT_D AND DAR.EXPY_D)
         OR (DAR.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
             )


             ;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR SELECT DAR.MERC_ACCT_I AS ACCT_I, AP.PATY_I, DAR.MAS_MERC_ACCT_I, AP.PATY_ACCT_REL_C, 'N' AS PRFR_PATY_I, AP.SRCE_SYST_C, (CASE WHEN AP.EFFT_D > DAR.EFFT_D THEN AP.EFFT_D ELSE DAR.EFFT_D END) AS EFFT_D, (CASE WHEN AP.EXPY_D < DAR.EXPY_D THEN AP.EXPY_D ELSE DAR.EXPY_D END) AS EXPY_D, AP.ROW_SECU_ACCS_C FROM (SELECT ACCT_I, PATY_I, PATY_ACCT_REL_C, SRCE_SYST_C, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D, ROW_SECU_ACCS_C FROM %%DDSTG%%.ACCT_PATY_DEDUP WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS AP JOIN (SELECT DA.MERC_ACCT_I, AX.MAS_MERC_ACCT_I, (CASE WHEN DA.EFFT_D > AX.EFFT_D THEN DA.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D, (CASE WHEN DA.EXPY_D < AX.EXPY_D THEN DA.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D FROM (SELECT MERC_ACCT_I, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D FROM %%VTECH%%.DAR_ACCT WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS DA JOIN (SELECT MAS_MERC_ACCT_I, DAR_ACCT_I, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D FROM %%VTECH%%.ACCT_XREF_MAS_DAR WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS AX ON AX.DAR_ACCT_I = DA.MERC_ACCT_I WHERE ((AX.EFFT_D BETWEEN DA.EFFT_D AND DA.EXPY_D) OR (DA.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D)) GROUP BY 1, 2, 3, 4) AS DAR ON DAR.MAS_MERC_ACCT_I = AP.ACCT_I WHERE ((AP.EFFT_D BETWEEN DAR.EFFT_D AND DAR.EXPY_D) OR (DAR.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D))
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."derv_acct_paty_curr"
WITH "dar" AS (
  SELECT
    "dar_acct"."merc_acct_i" AS "merc_acct_i",
    "acct_xref_mas_dar"."mas_merc_acct_i" AS "mas_merc_acct_i",
    (
      CASE
        WHEN "dar_acct"."efft_d" > "acct_xref_mas_dar"."efft_d"
        THEN "dar_acct"."efft_d"
        ELSE "acct_xref_mas_dar"."efft_d"
      END
    ) AS "efft_d",
    (
      CASE
        WHEN CASE
          WHEN "dar_acct"."efft_d" = "dar_acct"."expy_d"
          THEN "dar_acct"."expy_d"
          WHEN "dar_acct"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "dar_acct"."expy_d"
        END < CASE
          WHEN "acct_xref_mas_dar"."efft_d" = "acct_xref_mas_dar"."expy_d"
          THEN "acct_xref_mas_dar"."expy_d"
          WHEN "acct_xref_mas_dar"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "acct_xref_mas_dar"."expy_d"
        END
        THEN CASE
          WHEN "dar_acct"."efft_d" = "dar_acct"."expy_d"
          THEN "dar_acct"."expy_d"
          WHEN "dar_acct"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "dar_acct"."expy_d"
        END
        ELSE CASE
          WHEN "acct_xref_mas_dar"."efft_d" = "acct_xref_mas_dar"."expy_d"
          THEN "acct_xref_mas_dar"."expy_d"
          WHEN "acct_xref_mas_dar"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "acct_xref_mas_dar"."expy_d"
        END
      END
    ) AS "expy_d"
  FROM "%%vtech%%"."dar_acct" AS "dar_acct"
  JOIN "%%vtech%%"."acct_xref_mas_dar" AS "acct_xref_mas_dar"
    ON "acct_xref_mas_dar"."dar_acct_i" = "dar_acct"."merc_acct_i"
    AND "acct_xref_mas_dar"."efft_d" <= :EXTR_D
    AND (
      (
        "acct_xref_mas_dar"."efft_d" <= "dar_acct"."efft_d"
        AND "dar_acct"."efft_d" <= CASE
          WHEN "acct_xref_mas_dar"."efft_d" = "acct_xref_mas_dar"."expy_d"
          THEN "acct_xref_mas_dar"."expy_d"
          WHEN "acct_xref_mas_dar"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "acct_xref_mas_dar"."expy_d"
        END
      )
      OR (
        "acct_xref_mas_dar"."efft_d" <= CASE
          WHEN "dar_acct"."efft_d" = "dar_acct"."expy_d"
          THEN "dar_acct"."expy_d"
          WHEN "dar_acct"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "dar_acct"."expy_d"
        END
        AND "acct_xref_mas_dar"."efft_d" >= "dar_acct"."efft_d"
      )
    )
    AND CASE
      WHEN "acct_xref_mas_dar"."efft_d" = "acct_xref_mas_dar"."expy_d"
      THEN "acct_xref_mas_dar"."expy_d"
      WHEN "acct_xref_mas_dar"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "acct_xref_mas_dar"."expy_d"
    END >= :EXTR_D
  WHERE
    "dar_acct"."efft_d" <= :EXTR_D
    AND (
      (
        "acct_xref_mas_dar"."efft_d" <= "dar_acct"."efft_d"
        AND "dar_acct"."efft_d" <= CASE
          WHEN "acct_xref_mas_dar"."efft_d" = "acct_xref_mas_dar"."expy_d"
          THEN "acct_xref_mas_dar"."expy_d"
          WHEN "acct_xref_mas_dar"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "acct_xref_mas_dar"."expy_d"
        END
      )
      OR (
        "acct_xref_mas_dar"."efft_d" <= CASE
          WHEN "dar_acct"."efft_d" = "dar_acct"."expy_d"
          THEN "dar_acct"."expy_d"
          WHEN "dar_acct"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "dar_acct"."expy_d"
        END
        AND "acct_xref_mas_dar"."efft_d" >= "dar_acct"."efft_d"
      )
    )
    AND CASE
      WHEN "dar_acct"."efft_d" = "dar_acct"."expy_d"
      THEN "dar_acct"."expy_d"
      WHEN "dar_acct"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "dar_acct"."expy_d"
    END >= :EXTR_D
  GROUP BY
    "dar_acct"."merc_acct_i",
    "acct_xref_mas_dar"."mas_merc_acct_i",
    (
      CASE
        WHEN "dar_acct"."efft_d" > "acct_xref_mas_dar"."efft_d"
        THEN "dar_acct"."efft_d"
        ELSE "acct_xref_mas_dar"."efft_d"
      END
    ),
    (
      CASE
        WHEN CASE
          WHEN "dar_acct"."efft_d" = "dar_acct"."expy_d"
          THEN "dar_acct"."expy_d"
          WHEN "dar_acct"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "dar_acct"."expy_d"
        END < CASE
          WHEN "acct_xref_mas_dar"."efft_d" = "acct_xref_mas_dar"."expy_d"
          THEN "acct_xref_mas_dar"."expy_d"
          WHEN "acct_xref_mas_dar"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "acct_xref_mas_dar"."expy_d"
        END
        THEN CASE
          WHEN "dar_acct"."efft_d" = "dar_acct"."expy_d"
          THEN "dar_acct"."expy_d"
          WHEN "dar_acct"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "dar_acct"."expy_d"
        END
        ELSE CASE
          WHEN "acct_xref_mas_dar"."efft_d" = "acct_xref_mas_dar"."expy_d"
          THEN "acct_xref_mas_dar"."expy_d"
          WHEN "acct_xref_mas_dar"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "acct_xref_mas_dar"."expy_d"
        END
      END
    )
)
SELECT
  "dar"."merc_acct_i" AS "acct_i",
  "acct_paty_dedup"."paty_i" AS "paty_i",
  "dar"."mas_merc_acct_i" AS "mas_merc_acct_i",
  "acct_paty_dedup"."paty_acct_rel_c" AS "paty_acct_rel_c",
  'N' AS "prfr_paty_i",
  "acct_paty_dedup"."srce_syst_c" AS "srce_syst_c",
  CASE
    WHEN "acct_paty_dedup"."efft_d" > "dar"."efft_d"
    THEN "acct_paty_dedup"."efft_d"
    ELSE "dar"."efft_d"
  END AS "efft_d",
  CASE
    WHEN "dar"."expy_d" > CASE
      WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
      THEN "acct_paty_dedup"."expy_d"
      WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "acct_paty_dedup"."expy_d"
    END
    THEN CASE
      WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
      THEN "acct_paty_dedup"."expy_d"
      WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "acct_paty_dedup"."expy_d"
    END
    ELSE "dar"."expy_d"
  END AS "expy_d",
  "acct_paty_dedup"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "%%ddstg%%"."acct_paty_dedup" AS "acct_paty_dedup"
JOIN "dar" AS "dar"
  ON "acct_paty_dedup"."acct_i" = "dar"."mas_merc_acct_i"
  AND (
    (
      "acct_paty_dedup"."efft_d" <= "dar"."efft_d"
      AND "dar"."efft_d" <= CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
    )
    OR (
      "acct_paty_dedup"."efft_d" <= "dar"."expy_d"
      AND "acct_paty_dedup"."efft_d" >= "dar"."efft_d"
    )
  )
WHERE
  "acct_paty_dedup"."efft_d" <= :EXTR_D
  AND (
    (
      "acct_paty_dedup"."efft_d" <= "dar"."efft_d"
      AND "dar"."efft_d" <= CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
    )
    OR (
      "acct_paty_dedup"."efft_d" <= "dar"."expy_d"
      AND "acct_paty_dedup"."efft_d" >= "dar"."efft_d"
    )
  )
  AND CASE
    WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
    THEN "acct_paty_dedup"."expy_d"
    WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
    THEN CAST('9999-12-31' AS DATE)
    ELSE "acct_paty_dedup"."expy_d"
  END >= :EXTR_D
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_PATY_DEDUP, ACCT_XREF_MAS_DAR, DAR_ACCT, DERV_ACCT_PATY_CURR
- **Columns**: ACCT_I, DAR_ACCT_I, EFFT_D, EXPY_D, MAS_MERC_ACCT_I, MERC_ACCT_I, PATY_ACCT_REL_C, PATY_I, ROW_SECU_ACCS_C, SRCE_SYST_C
- **Functions**: '9999-12-31', (AP.EFFT_D BETWEEN DAR.EFFT_D AND DAR.EXPY_D), (AX.EFFT_D BETWEEN DA.EFFT_D AND DA.EXPY_D), AP.EFFT_D > DAR.EFFT_D, AP.EXPY_D < DAR.EXPY_D, DA.EFFT_D > AX.EFFT_D, DA.EXPY_D < AX.EXPY_D, EFFT_D = EXPY_D, EXPY_D >= :EXTR_D, None

### SQL Block 8 (Lines 447-495)
- **Complexity Score**: 231
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR
SELECT AR.OBJC_ACCT_I AS ACCT_I
     , AP.PATY_I
     ,AR.SUBJ_ACCT_I AS ASSC_ACCT_I
    , AP.PATY_ACCT_REL_C
    ,'N' AS PRFR_PATY_F
    , AP.SRCE_SYST_C
, (CASE WHEN AR.EFFT_D >  AP.EFFT_D  THEN AR.EFFT_D  ELSE AP.EFFT_D END) AS EFFT_D
, (CASE WHEN AR.EXPY_D <  AP.EXPY_D THEN AR.EXPY_D ELSE AP.EXPY_D END) AS EXPY_D
 ,AP.ROW_SECU_ACCS_C
FROM  (
                SEL ACCT_I
                     ,PATY_I
                     ,PATY_ACCT_REL_C
                     ,SRCE_SYST_C
                     ,EFFT_D
                     ,CASE
                               WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                                 ELSE EXPY_D
                        END AS EXPY_D
                     ,ROW_SECU_ACCS_C       
           FROM    %%DDSTG%%.ACCT_PATY_DEDUP
             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP 

JOIN (
            SELECT AR1.SUBJ_ACCT_I
                          ,AR1.OBJC_ACCT_I
                          ,AR1.EFFT_D
                          ,CASE
                                WHEN AR1.EFFT_D = AR1.EXPY_D THEN AR1.EXPY_D
                                WHEN AR1.EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                                 ELSE AR1.EXPY_D
                        END AS EXPY_D
                     FROM %%VTECH%%.ACCT_REL AR1
                        JOIN %%DDSTG%%.GRD_GNRC_MAP_DERV_PATY_REL GGM
                        ON AR1.REL_C = GGM.REL_C
                     AND GGM.ACCT_I_C = 'SUBJ'   
                     AND :EXTR_D BETWEEN AR1.EFFT_D AND AR1.EXPY_D ) AR
  ON AR.SUBJ_ACCT_I = AP.ACCT_I 
      
WHERE ( (AR.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D) 
        OR  (AP.EFFT_D BETWEEN AR.EFFT_D AND AR.EXPY_D) 
    )
    
   
  
GROUP BY 1,2,3,4,5,6,7,8,9
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR SELECT AR.OBJC_ACCT_I AS ACCT_I, AP.PATY_I, AR.SUBJ_ACCT_I AS ASSC_ACCT_I, AP.PATY_ACCT_REL_C, 'N' AS PRFR_PATY_F, AP.SRCE_SYST_C, (CASE WHEN AR.EFFT_D > AP.EFFT_D THEN AR.EFFT_D ELSE AP.EFFT_D END) AS EFFT_D, (CASE WHEN AR.EXPY_D < AP.EXPY_D THEN AR.EXPY_D ELSE AP.EXPY_D END) AS EXPY_D, AP.ROW_SECU_ACCS_C FROM (SELECT ACCT_I, PATY_I, PATY_ACCT_REL_C, SRCE_SYST_C, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D, ROW_SECU_ACCS_C FROM %%DDSTG%%.ACCT_PATY_DEDUP WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS AP JOIN (SELECT AR1.SUBJ_ACCT_I, AR1.OBJC_ACCT_I, AR1.EFFT_D, CASE WHEN AR1.EFFT_D = AR1.EXPY_D THEN AR1.EXPY_D WHEN AR1.EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE AR1.EXPY_D END AS EXPY_D FROM %%VTECH%%.ACCT_REL AS AR1 JOIN %%DDSTG%%.GRD_GNRC_MAP_DERV_PATY_REL AS GGM ON AR1.REL_C = GGM.REL_C AND GGM.ACCT_I_C = 'SUBJ' AND :EXTR_D BETWEEN AR1.EFFT_D AND AR1.EXPY_D) AS AR ON AR.SUBJ_ACCT_I = AP.ACCT_I WHERE ((AR.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D) OR (AP.EFFT_D BETWEEN AR.EFFT_D AND AR.EXPY_D)) GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."derv_acct_paty_curr"
SELECT
  "ar1"."objc_acct_i" AS "acct_i",
  "acct_paty_dedup"."paty_i" AS "paty_i",
  "ar1"."subj_acct_i" AS "assc_acct_i",
  "acct_paty_dedup"."paty_acct_rel_c" AS "paty_acct_rel_c",
  'N' AS "prfr_paty_f",
  "acct_paty_dedup"."srce_syst_c" AS "srce_syst_c",
  (
    CASE
      WHEN "ar1"."efft_d" > "acct_paty_dedup"."efft_d"
      THEN "ar1"."efft_d"
      ELSE "acct_paty_dedup"."efft_d"
    END
  ) AS "efft_d",
  (
    CASE
      WHEN CASE
        WHEN "ar1"."efft_d" = "ar1"."expy_d"
        THEN "ar1"."expy_d"
        WHEN "ar1"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "ar1"."expy_d"
      END < CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
      THEN CASE
        WHEN "ar1"."efft_d" = "ar1"."expy_d"
        THEN "ar1"."expy_d"
        WHEN "ar1"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "ar1"."expy_d"
      END
      ELSE CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
    END
  ) AS "expy_d",
  "acct_paty_dedup"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "%%ddstg%%"."acct_paty_dedup" AS "acct_paty_dedup"
JOIN "%%vtech%%"."acct_rel" AS "ar1"
  ON "acct_paty_dedup"."acct_i" = "ar1"."subj_acct_i"
  AND (
    (
      "acct_paty_dedup"."efft_d" <= "ar1"."efft_d"
      AND "ar1"."efft_d" <= CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
    )
    OR (
      "acct_paty_dedup"."efft_d" <= CASE
        WHEN "ar1"."efft_d" = "ar1"."expy_d"
        THEN "ar1"."expy_d"
        WHEN "ar1"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "ar1"."expy_d"
      END
      AND "acct_paty_dedup"."efft_d" >= "ar1"."efft_d"
    )
  )
JOIN "%%ddstg%%"."grd_gnrc_map_derv_paty_rel" AS "ggm"
  ON "ar1"."efft_d" <= :EXTR_D
  AND "ar1"."expy_d" >= :EXTR_D
  AND "ar1"."rel_c" = "ggm"."rel_c"
  AND "ggm"."acct_i_c" = 'SUBJ'
WHERE
  "acct_paty_dedup"."efft_d" <= :EXTR_D
  AND (
    (
      "acct_paty_dedup"."efft_d" <= "ar1"."efft_d"
      AND "ar1"."efft_d" <= CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
    )
    OR (
      "acct_paty_dedup"."efft_d" <= CASE
        WHEN "ar1"."efft_d" = "ar1"."expy_d"
        THEN "ar1"."expy_d"
        WHEN "ar1"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "ar1"."expy_d"
      END
      AND "acct_paty_dedup"."efft_d" >= "ar1"."efft_d"
    )
  )
  AND CASE
    WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
    THEN "acct_paty_dedup"."expy_d"
    WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
    THEN CAST('9999-12-31' AS DATE)
    ELSE "acct_paty_dedup"."expy_d"
  END >= :EXTR_D
GROUP BY
  "ar1"."objc_acct_i",
  "acct_paty_dedup"."paty_i",
  "ar1"."subj_acct_i",
  "acct_paty_dedup"."paty_acct_rel_c",
  5,
  "acct_paty_dedup"."srce_syst_c",
  (
    CASE
      WHEN "ar1"."efft_d" > "acct_paty_dedup"."efft_d"
      THEN "ar1"."efft_d"
      ELSE "acct_paty_dedup"."efft_d"
    END
  ),
  (
    CASE
      WHEN CASE
        WHEN "ar1"."efft_d" = "ar1"."expy_d"
        THEN "ar1"."expy_d"
        WHEN "ar1"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "ar1"."expy_d"
      END < CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
      THEN CASE
        WHEN "ar1"."efft_d" = "ar1"."expy_d"
        THEN "ar1"."expy_d"
        WHEN "ar1"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "ar1"."expy_d"
      END
      ELSE CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
    END
  ),
  "acct_paty_dedup"."row_secu_accs_c"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_PATY_DEDUP, ACCT_REL, DERV_ACCT_PATY_CURR, GRD_GNRC_MAP_DERV_PATY_REL
- **Columns**: ACCT_I, ACCT_I_C, EFFT_D, EXPY_D, OBJC_ACCT_I, PATY_ACCT_REL_C, PATY_I, REL_C, ROW_SECU_ACCS_C, SRCE_SYST_C, SUBJ_ACCT_I
- **Functions**: '9999-12-31', (AR.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D), AR.EFFT_D > AP.EFFT_D, AR.EXPY_D < AP.EXPY_D, AR1.EFFT_D = AR1.EXPY_D, AR1.EXPY_D >= :EXTR_D, AR1.REL_C = GGM.REL_C, AR1.REL_C = GGM.REL_C AND GGM.ACCT_I_C = 'SUBJ', EFFT_D = EXPY_D, EXPY_D >= :EXTR_D, None

### SQL Block 9 (Lines 508-555)
- **Complexity Score**: 231
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR
SELECT AR.SUBJ_ACCT_I AS ACCT_I
     , AP.PATY_I
     ,AR.OBJC_ACCT_I AS ASSC_ACCT_I
    , AP.PATY_ACCT_REL_C
    ,'N' AS PRFR_PATY_F
    , AP.SRCE_SYST_C
, (CASE WHEN AR.EFFT_D >  AP.EFFT_D  THEN AR.EFFT_D  ELSE AP.EFFT_D END) AS EFFT_D
, (CASE WHEN AR.EXPY_D <  AP.EXPY_D THEN AR.EXPY_D ELSE AP.EXPY_D END) AS EXPY_D
 ,AP.ROW_SECU_ACCS_C
FROM (
                SEL ACCT_I
                     ,PATY_I
                     ,PATY_ACCT_REL_C
                     ,SRCE_SYST_C
                     ,EFFT_D
                     ,CASE
                               WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                                 ELSE EXPY_D
                        END AS EXPY_D
                     ,ROW_SECU_ACCS_C       
           FROM    %%DDSTG%%.ACCT_PATY_DEDUP
             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP

JOIN (
            SELECT AR1.SUBJ_ACCT_I
                          ,AR1.OBJC_ACCT_I
                          ,AR1.EFFT_D
                          ,CASE
                                WHEN AR1.EFFT_D = AR1.EXPY_D THEN AR1.EXPY_D
                                WHEN AR1.EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                                 ELSE AR1.EXPY_D
                        END AS EXPY_D
                     FROM %%VTECH%%.ACCT_REL AR1
                        JOIN %%DDSTG%%.GRD_GNRC_MAP_DERV_PATY_REL GGM
                        ON AR1.REL_C = GGM.REL_C
                     AND GGM.ACCT_I_C = 'OBJC'   
                     AND :EXTR_D BETWEEN AR1.EFFT_D AND AR1.EXPY_D ) AR
  ON AR.OBJC_ACCT_I = AP.ACCT_I 
      
WHERE ( (AR.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D) 
        OR  (AP.EFFT_D BETWEEN AR.EFFT_D AND AR.EXPY_D) 
    )
    
   
GROUP BY 1,2,3,4,5,6,7,8,9  
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR SELECT AR.SUBJ_ACCT_I AS ACCT_I, AP.PATY_I, AR.OBJC_ACCT_I AS ASSC_ACCT_I, AP.PATY_ACCT_REL_C, 'N' AS PRFR_PATY_F, AP.SRCE_SYST_C, (CASE WHEN AR.EFFT_D > AP.EFFT_D THEN AR.EFFT_D ELSE AP.EFFT_D END) AS EFFT_D, (CASE WHEN AR.EXPY_D < AP.EXPY_D THEN AR.EXPY_D ELSE AP.EXPY_D END) AS EXPY_D, AP.ROW_SECU_ACCS_C FROM (SELECT ACCT_I, PATY_I, PATY_ACCT_REL_C, SRCE_SYST_C, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D, ROW_SECU_ACCS_C FROM %%DDSTG%%.ACCT_PATY_DEDUP WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS AP JOIN (SELECT AR1.SUBJ_ACCT_I, AR1.OBJC_ACCT_I, AR1.EFFT_D, CASE WHEN AR1.EFFT_D = AR1.EXPY_D THEN AR1.EXPY_D WHEN AR1.EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE AR1.EXPY_D END AS EXPY_D FROM %%VTECH%%.ACCT_REL AS AR1 JOIN %%DDSTG%%.GRD_GNRC_MAP_DERV_PATY_REL AS GGM ON AR1.REL_C = GGM.REL_C AND GGM.ACCT_I_C = 'OBJC' AND :EXTR_D BETWEEN AR1.EFFT_D AND AR1.EXPY_D) AS AR ON AR.OBJC_ACCT_I = AP.ACCT_I WHERE ((AR.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D) OR (AP.EFFT_D BETWEEN AR.EFFT_D AND AR.EXPY_D)) GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."derv_acct_paty_curr"
SELECT
  "ar1"."subj_acct_i" AS "acct_i",
  "acct_paty_dedup"."paty_i" AS "paty_i",
  "ar1"."objc_acct_i" AS "assc_acct_i",
  "acct_paty_dedup"."paty_acct_rel_c" AS "paty_acct_rel_c",
  'N' AS "prfr_paty_f",
  "acct_paty_dedup"."srce_syst_c" AS "srce_syst_c",
  (
    CASE
      WHEN "ar1"."efft_d" > "acct_paty_dedup"."efft_d"
      THEN "ar1"."efft_d"
      ELSE "acct_paty_dedup"."efft_d"
    END
  ) AS "efft_d",
  (
    CASE
      WHEN CASE
        WHEN "ar1"."efft_d" = "ar1"."expy_d"
        THEN "ar1"."expy_d"
        WHEN "ar1"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "ar1"."expy_d"
      END < CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
      THEN CASE
        WHEN "ar1"."efft_d" = "ar1"."expy_d"
        THEN "ar1"."expy_d"
        WHEN "ar1"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "ar1"."expy_d"
      END
      ELSE CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
    END
  ) AS "expy_d",
  "acct_paty_dedup"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "%%ddstg%%"."acct_paty_dedup" AS "acct_paty_dedup"
JOIN "%%vtech%%"."acct_rel" AS "ar1"
  ON "acct_paty_dedup"."acct_i" = "ar1"."objc_acct_i"
  AND (
    (
      "acct_paty_dedup"."efft_d" <= "ar1"."efft_d"
      AND "ar1"."efft_d" <= CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
    )
    OR (
      "acct_paty_dedup"."efft_d" <= CASE
        WHEN "ar1"."efft_d" = "ar1"."expy_d"
        THEN "ar1"."expy_d"
        WHEN "ar1"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "ar1"."expy_d"
      END
      AND "acct_paty_dedup"."efft_d" >= "ar1"."efft_d"
    )
  )
JOIN "%%ddstg%%"."grd_gnrc_map_derv_paty_rel" AS "ggm"
  ON "ar1"."efft_d" <= :EXTR_D
  AND "ar1"."expy_d" >= :EXTR_D
  AND "ar1"."rel_c" = "ggm"."rel_c"
  AND "ggm"."acct_i_c" = 'OBJC'
WHERE
  "acct_paty_dedup"."efft_d" <= :EXTR_D
  AND (
    (
      "acct_paty_dedup"."efft_d" <= "ar1"."efft_d"
      AND "ar1"."efft_d" <= CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
    )
    OR (
      "acct_paty_dedup"."efft_d" <= CASE
        WHEN "ar1"."efft_d" = "ar1"."expy_d"
        THEN "ar1"."expy_d"
        WHEN "ar1"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "ar1"."expy_d"
      END
      AND "acct_paty_dedup"."efft_d" >= "ar1"."efft_d"
    )
  )
  AND CASE
    WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
    THEN "acct_paty_dedup"."expy_d"
    WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
    THEN CAST('9999-12-31' AS DATE)
    ELSE "acct_paty_dedup"."expy_d"
  END >= :EXTR_D
GROUP BY
  "ar1"."subj_acct_i",
  "acct_paty_dedup"."paty_i",
  "ar1"."objc_acct_i",
  "acct_paty_dedup"."paty_acct_rel_c",
  5,
  "acct_paty_dedup"."srce_syst_c",
  (
    CASE
      WHEN "ar1"."efft_d" > "acct_paty_dedup"."efft_d"
      THEN "ar1"."efft_d"
      ELSE "acct_paty_dedup"."efft_d"
    END
  ),
  (
    CASE
      WHEN CASE
        WHEN "ar1"."efft_d" = "ar1"."expy_d"
        THEN "ar1"."expy_d"
        WHEN "ar1"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "ar1"."expy_d"
      END < CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
      THEN CASE
        WHEN "ar1"."efft_d" = "ar1"."expy_d"
        THEN "ar1"."expy_d"
        WHEN "ar1"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "ar1"."expy_d"
      END
      ELSE CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
    END
  ),
  "acct_paty_dedup"."row_secu_accs_c"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_PATY_DEDUP, ACCT_REL, DERV_ACCT_PATY_CURR, GRD_GNRC_MAP_DERV_PATY_REL
- **Columns**: ACCT_I, ACCT_I_C, EFFT_D, EXPY_D, OBJC_ACCT_I, PATY_ACCT_REL_C, PATY_I, REL_C, ROW_SECU_ACCS_C, SRCE_SYST_C, SUBJ_ACCT_I
- **Functions**: '9999-12-31', (AR.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D), AR.EFFT_D > AP.EFFT_D, AR.EXPY_D < AP.EXPY_D, AR1.EFFT_D = AR1.EXPY_D, AR1.EXPY_D >= :EXTR_D, AR1.REL_C = GGM.REL_C, AR1.REL_C = GGM.REL_C AND GGM.ACCT_I_C = 'OBJC', EFFT_D = EXPY_D, EXPY_D >= :EXTR_D, None

### SQL Block 10 (Lines 574-619)
- **Complexity Score**: 208
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.ACCT_PATY_REL_WSS
SELECT AR.SUBJ_ACCT_I AS ACCT_I
            , AP.PATY_I
             ,AR.OBJC_ACCT_I AS ASSC_ACCT_I
            , AP.PATY_ACCT_REL_C
            ,'N' AS PRFR_PATY_F
            , AP.SRCE_SYST_C
            , (CASE WHEN AR.EFFT_D >  AP.EFFT_D  THEN AR.EFFT_D  ELSE AP.EFFT_D END) AS EFFT_D
           , (CASE WHEN AR.EXPY_D <  AP.EXPY_D THEN AR.EXPY_D ELSE AP.EXPY_D END) AS EXPY_D
           ,AP.ROW_SECU_ACCS_C
           
 FROM (
                SEL ACCT_I
                     ,PATY_I
                     ,PATY_ACCT_REL_C
                     ,SRCE_SYST_C
                     ,EFFT_D
                     ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                                 ELSE EXPY_D
                        END AS EXPY_D
                     ,ROW_SECU_ACCS_C       
           FROM    %%DDSTG%%.ACCT_PATY_DEDUP
             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP
   JOIN  (
            SELECT AR1.SUBJ_ACCT_I
                          ,AR1.OBJC_ACCT_I
                          ,AR1.EFFT_D
                          ,CASE
                                WHEN AR1.EFFT_D = AR1.EXPY_D THEN AR1.EXPY_D
                                WHEN AR1.EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                                 ELSE AR1.EXPY_D
                        END AS EXPY_D
                     FROM %%VTECH%%.ACCT_REL AR1
                      WHERE  AR1.REL_C = 'FCLYO'     
                     AND :EXTR_D BETWEEN AR1.EFFT_D AND AR1.EXPY_D ) AR 
     ON AR.OBJC_ACCT_I = AP.ACCT_I 
   
WHERE ( (AR.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D) 
        OR  (AP.EFFT_D BETWEEN AR.EFFT_D AND AR.EXPY_D) 
        )
 

;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.ACCT_PATY_REL_WSS SELECT AR.SUBJ_ACCT_I AS ACCT_I, AP.PATY_I, AR.OBJC_ACCT_I AS ASSC_ACCT_I, AP.PATY_ACCT_REL_C, 'N' AS PRFR_PATY_F, AP.SRCE_SYST_C, (CASE WHEN AR.EFFT_D > AP.EFFT_D THEN AR.EFFT_D ELSE AP.EFFT_D END) AS EFFT_D, (CASE WHEN AR.EXPY_D < AP.EXPY_D THEN AR.EXPY_D ELSE AP.EXPY_D END) AS EXPY_D, AP.ROW_SECU_ACCS_C FROM (SELECT ACCT_I, PATY_I, PATY_ACCT_REL_C, SRCE_SYST_C, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D, ROW_SECU_ACCS_C FROM %%DDSTG%%.ACCT_PATY_DEDUP WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS AP JOIN (SELECT AR1.SUBJ_ACCT_I, AR1.OBJC_ACCT_I, AR1.EFFT_D, CASE WHEN AR1.EFFT_D = AR1.EXPY_D THEN AR1.EXPY_D WHEN AR1.EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE AR1.EXPY_D END AS EXPY_D FROM %%VTECH%%.ACCT_REL AS AR1 WHERE AR1.REL_C = 'FCLYO' AND :EXTR_D BETWEEN AR1.EFFT_D AND AR1.EXPY_D) AS AR ON AR.OBJC_ACCT_I = AP.ACCT_I WHERE ((AR.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D) OR (AP.EFFT_D BETWEEN AR.EFFT_D AND AR.EXPY_D))
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."acct_paty_rel_wss"
SELECT
  "ar1"."subj_acct_i" AS "acct_i",
  "acct_paty_dedup"."paty_i" AS "paty_i",
  "ar1"."objc_acct_i" AS "assc_acct_i",
  "acct_paty_dedup"."paty_acct_rel_c" AS "paty_acct_rel_c",
  'N' AS "prfr_paty_f",
  "acct_paty_dedup"."srce_syst_c" AS "srce_syst_c",
  CASE
    WHEN "acct_paty_dedup"."efft_d" < "ar1"."efft_d"
    THEN "ar1"."efft_d"
    ELSE "acct_paty_dedup"."efft_d"
  END AS "efft_d",
  CASE
    WHEN CASE
      WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
      THEN "acct_paty_dedup"."expy_d"
      WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "acct_paty_dedup"."expy_d"
    END > CASE
      WHEN "ar1"."efft_d" = "ar1"."expy_d"
      THEN "ar1"."expy_d"
      WHEN "ar1"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "ar1"."expy_d"
    END
    THEN CASE
      WHEN "ar1"."efft_d" = "ar1"."expy_d"
      THEN "ar1"."expy_d"
      WHEN "ar1"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "ar1"."expy_d"
    END
    ELSE CASE
      WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
      THEN "acct_paty_dedup"."expy_d"
      WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "acct_paty_dedup"."expy_d"
    END
  END AS "expy_d",
  "acct_paty_dedup"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "%%ddstg%%"."acct_paty_dedup" AS "acct_paty_dedup"
JOIN "%%vtech%%"."acct_rel" AS "ar1"
  ON "acct_paty_dedup"."acct_i" = "ar1"."objc_acct_i"
  AND "ar1"."efft_d" <= :EXTR_D
  AND "ar1"."expy_d" >= :EXTR_D
  AND "ar1"."rel_c" = 'FCLYO'
  AND (
    (
      "acct_paty_dedup"."efft_d" <= "ar1"."efft_d"
      AND "ar1"."efft_d" <= CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
    )
    OR (
      "acct_paty_dedup"."efft_d" <= CASE
        WHEN "ar1"."efft_d" = "ar1"."expy_d"
        THEN "ar1"."expy_d"
        WHEN "ar1"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "ar1"."expy_d"
      END
      AND "acct_paty_dedup"."efft_d" >= "ar1"."efft_d"
    )
  )
WHERE
  "acct_paty_dedup"."efft_d" <= :EXTR_D
  AND (
    (
      "acct_paty_dedup"."efft_d" <= "ar1"."efft_d"
      AND "ar1"."efft_d" <= CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
    )
    OR (
      "acct_paty_dedup"."efft_d" <= CASE
        WHEN "ar1"."efft_d" = "ar1"."expy_d"
        THEN "ar1"."expy_d"
        WHEN "ar1"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "ar1"."expy_d"
      END
      AND "acct_paty_dedup"."efft_d" >= "ar1"."efft_d"
    )
  )
  AND CASE
    WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
    THEN "acct_paty_dedup"."expy_d"
    WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
    THEN CAST('9999-12-31' AS DATE)
    ELSE "acct_paty_dedup"."expy_d"
  END >= :EXTR_D
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_PATY_DEDUP, ACCT_PATY_REL_WSS, ACCT_REL
- **Columns**: ACCT_I, EFFT_D, EXPY_D, OBJC_ACCT_I, PATY_ACCT_REL_C, PATY_I, REL_C, ROW_SECU_ACCS_C, SRCE_SYST_C, SUBJ_ACCT_I
- **Functions**: '9999-12-31', (AR.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D), AR.EFFT_D > AP.EFFT_D, AR.EXPY_D < AP.EXPY_D, AR1.EFFT_D = AR1.EXPY_D, AR1.EXPY_D >= :EXTR_D, AR1.REL_C = 'FCLYO', EFFT_D = EXPY_D, EXPY_D >= :EXTR_D, None

### SQL Block 11 (Lines 629-630)
- **Complexity Score**: 4
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
DELETE FROM %%DDSTG%%.ACCT_REL_WSS_DITPS;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%DDSTG%%.ACCT_REL_WSS_DITPS
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%ddstg%%"."acct_rel_wss_ditps"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_REL_WSS_DITPS

### SQL Block 12 (Lines 634-653)
- **Complexity Score**: 109
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.ACCT_REL_WSS_DITPS
 SEL APA.ACCT_I
  FROM %%DDSTG%%. ACCT_PATY_REL_WSS APA
     JOIN   (
            SELECT AR1.SUBJ_ACCT_I
                          ,AR1.OBJC_ACCT_I
                          ,AR1.EFFT_D
                          ,CASE
                                WHEN AR1.EFFT_D = AR1.EXPY_D THEN AR1.EXPY_D
                                WHEN AR1.EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                                 ELSE AR1.EXPY_D
                        END AS EXPY_D
                     FROM %%VTECH%%.ACCT_REL AR1
                      WHERE  AR1.REL_C = 'DITPS'
                     AND :EXTR_D BETWEEN AR1.EFFT_D AND AR1.EXPY_D ) AR
        ON APA.ACCT_I = AR.OBJC_ACCT_I
       AND (  (APA.EFFT_D BETWEEN AR.EFFT_D AND AR.EXPY_D) 
       OR  (AR.EFFT_D BETWEEN APA.EFFT_D AND APA.EXPY_D))
 
        GROUP BY 1 ;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.ACCT_REL_WSS_DITPS SELECT APA.ACCT_I FROM %%DDSTG%%.ACCT_PATY_REL_WSS AS APA JOIN (SELECT AR1.SUBJ_ACCT_I, AR1.OBJC_ACCT_I, AR1.EFFT_D, CASE WHEN AR1.EFFT_D = AR1.EXPY_D THEN AR1.EXPY_D WHEN AR1.EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE AR1.EXPY_D END AS EXPY_D FROM %%VTECH%%.ACCT_REL AS AR1 WHERE AR1.REL_C = 'DITPS' AND :EXTR_D BETWEEN AR1.EFFT_D AND AR1.EXPY_D) AS AR ON APA.ACCT_I = AR.OBJC_ACCT_I AND ((APA.EFFT_D BETWEEN AR.EFFT_D AND AR.EXPY_D) OR (AR.EFFT_D BETWEEN APA.EFFT_D AND APA.EXPY_D)) GROUP BY 1
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."acct_rel_wss_ditps"
SELECT
  "apa"."acct_i" AS "acct_i"
FROM "%%ddstg%%"."acct_paty_rel_wss" AS "apa"
JOIN "%%vtech%%"."acct_rel" AS "ar1"
  ON "apa"."acct_i" = "ar1"."objc_acct_i"
  AND "ar1"."efft_d" <= :EXTR_D
  AND "ar1"."expy_d" >= :EXTR_D
  AND "ar1"."rel_c" = 'DITPS'
  AND (
    (
      "apa"."efft_d" <= "ar1"."efft_d" AND "apa"."expy_d" >= "ar1"."efft_d"
    )
    OR (
      "apa"."efft_d" <= CASE
        WHEN "ar1"."efft_d" = "ar1"."expy_d"
        THEN "ar1"."expy_d"
        WHEN "ar1"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "ar1"."expy_d"
      END
      AND "apa"."efft_d" >= "ar1"."efft_d"
    )
  )
GROUP BY
  "apa"."acct_i"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_PATY_REL_WSS, ACCT_REL, ACCT_REL_WSS_DITPS
- **Columns**: ACCT_I, EFFT_D, EXPY_D, OBJC_ACCT_I, REL_C, SUBJ_ACCT_I
- **Functions**: '9999-12-31', (APA.EFFT_D BETWEEN AR.EFFT_D AND AR.EXPY_D), APA.ACCT_I = AR.OBJC_ACCT_I, AR1.EFFT_D = AR1.EXPY_D, AR1.EXPY_D >= :EXTR_D, AR1.REL_C = 'DITPS', None

### SQL Block 13 (Lines 678-760)
- **Complexity Score**: 367
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 3

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.ACCT_PATY_REL_WSS       

 SEL REL.ACCT_I
      ,AP.PATY_I
     ,REL.ASSC_ACCT_I 
     ,AP.PATY_ACCT_REL_C
     ,'N' AS PRFR_PATY_F  
     ,AP.SRCE_SYST_C
   , (CASE WHEN AP.EFFT_D > REL.EFFT_D THEN AP.EFFT_D ELSE REL.EFFT_D END) AS EFFT_D
   , (CASE WHEN AP.EXPY_D < REL.EXPY_D THEN AP.EXPY_D ELSE REL.EXPY_D END) AS EXPY_D

,AP.ROW_SECU_ACCS_C

FROM  (
                SEL ACCT_I
                     ,PATY_I
                     ,PATY_ACCT_REL_C
                     ,SRCE_SYST_C
                     ,EFFT_D
                     ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                                 ELSE EXPY_D
                        END AS EXPY_D
                     ,ROW_SECU_ACCS_C       
           FROM    %%DDSTG%%.ACCT_PATY_DEDUP
             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AP
JOIN 
    (SEL DITPS.OBJC_ACCT_I AS ACCT_I
        ,FCLYO.OBJC_ACCT_I AS ASSC_ACCT_I
    ,(CASE WHEN FCLYO.EFFT_D > DITPS.EFFT_D THEN FCLYO.EFFT_D ELSE DITPS.EFFT_D END) AS EFFT_D
    ,(CASE WHEN FCLYO.EXPY_D < DITPS.EXPY_D THEN FCLYO.EXPY_D ELSE DITPS.EXPY_D END) AS EXPY_D
     FROM  (
            SELECT SUBJ_ACCT_I
                          ,OBJC_ACCT_I
                          ,EFFT_D
                          ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                                 ELSE EXPY_D
                        END AS EXPY_D
                     FROM %%VTECH%%.ACCT_REL AR1
                      WHERE  REL_C = 'FCLYO'
                     AND :EXTR_D BETWEEN EFFT_D AND EXPY_D ) FCLYO

     JOIN     (
            SELECT SUBJ_ACCT_I
                          ,OBJC_ACCT_I
                          ,REL_C
                          ,EFFT_D
                          ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                                 ELSE EXPY_D
                        END AS EXPY_D
                     FROM %%VTECH%%.ACCT_REL AR1
                      WHERE  REL_C = 'DITPS'
                     AND :EXTR_D BETWEEN EFFT_D AND EXPY_D ) DITPS 
     ON FCLYO.SUBJ_ACCT_I = DITPS.OBJC_ACCT_I /* C2151903 - WSS DERV ACCT_PATY CHANGE */			 

     
     
     JOIN %%DDSTG%%.ACCT_REL_WSS_DITPS AR3
         ON DITPS.OBJC_ACCT_I = AR3.ACCT_I
         
     WHERE (
            (DITPS.EFFT_D BETWEEN FCLYO.EFFT_D AND FCLYO.EXPY_D) 
        OR (FCLYO.EFFT_D BETWEEN DITPS.EFFT_D AND DITPS.EXPY_D)
       ) 
 --this is to eliminate duplicate relationships that exist for some of DITPS 
   
       QUALIFY ROW_NUMBER() OVER(PARTITION BY DITPS.OBJC_ACCT_I, DITPS.REL_C ORDER BY DITPS.SUBJ_ACCT_I DESC) = 1
    
) AS REL   
ON REL.ASSC_ACCT_I = AP.ACCT_I

WHERE (
                (AP.EFFT_D BETWEEN REL.EFFT_D AND REL.EXPY_D) OR 
                (REL.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
              )

GROUP BY 1,2,3,4,5,6,7,8,9
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.ACCT_PATY_REL_WSS SELECT REL.ACCT_I, AP.PATY_I, REL.ASSC_ACCT_I, AP.PATY_ACCT_REL_C, 'N' AS PRFR_PATY_F, AP.SRCE_SYST_C, (CASE WHEN AP.EFFT_D > REL.EFFT_D THEN AP.EFFT_D ELSE REL.EFFT_D END) AS EFFT_D, (CASE WHEN AP.EXPY_D < REL.EXPY_D THEN AP.EXPY_D ELSE REL.EXPY_D END) AS EXPY_D, AP.ROW_SECU_ACCS_C FROM (SELECT ACCT_I, PATY_I, PATY_ACCT_REL_C, SRCE_SYST_C, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D, ROW_SECU_ACCS_C FROM %%DDSTG%%.ACCT_PATY_DEDUP WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS AP JOIN (SELECT DITPS.OBJC_ACCT_I AS ACCT_I, FCLYO.OBJC_ACCT_I AS ASSC_ACCT_I, (CASE WHEN FCLYO.EFFT_D > DITPS.EFFT_D THEN FCLYO.EFFT_D ELSE DITPS.EFFT_D END) AS EFFT_D, (CASE WHEN FCLYO.EXPY_D < DITPS.EXPY_D THEN FCLYO.EXPY_D ELSE DITPS.EXPY_D END) AS EXPY_D FROM (SELECT SUBJ_ACCT_I, OBJC_ACCT_I, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D FROM %%VTECH%%.ACCT_REL AS AR1 WHERE REL_C = 'FCLYO' AND :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS FCLYO JOIN (SELECT SUBJ_ACCT_I, OBJC_ACCT_I, REL_C, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D FROM %%VTECH%%.ACCT_REL AS AR1 WHERE REL_C = 'DITPS' AND :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS DITPS ON FCLYO.SUBJ_ACCT_I = DITPS.OBJC_ACCT_I JOIN %%DDSTG%%.ACCT_REL_WSS_DITPS AS AR3 ON DITPS.OBJC_ACCT_I = AR3.ACCT_I WHERE ((DITPS.EFFT_D BETWEEN FCLYO.EFFT_D AND FCLYO.EXPY_D) OR (FCLYO.EFFT_D BETWEEN DITPS.EFFT_D AND DITPS.EXPY_D)) QUALIFY ROW_NUMBER() OVER (PARTITION BY DITPS.OBJC_ACCT_I, DITPS.REL_C ORDER BY DITPS.SUBJ_ACCT_I DESC NULLS LAST) = 1) AS REL ON REL.ASSC_ACCT_I = AP.ACCT_I WHERE ((AP.EFFT_D BETWEEN REL.EFFT_D AND REL.EXPY_D) OR (REL.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)) GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- QUALIFY clause
- ROW_NUMBER() OVER
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."acct_paty_rel_wss"
WITH "rel" AS (
  SELECT
    "ar1_2"."objc_acct_i" AS "acct_i",
    "ar1"."objc_acct_i" AS "assc_acct_i",
    CASE
      WHEN "ar1"."efft_d" > "ar1_2"."efft_d"
      THEN "ar1"."efft_d"
      ELSE "ar1_2"."efft_d"
    END AS "efft_d",
    CASE
      WHEN CASE
        WHEN "ar1"."efft_d" = "ar1"."expy_d"
        THEN "ar1"."expy_d"
        WHEN "ar1"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "ar1"."expy_d"
      END < CASE
        WHEN "ar1_2"."efft_d" = "ar1_2"."expy_d"
        THEN "ar1_2"."expy_d"
        WHEN "ar1_2"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "ar1_2"."expy_d"
      END
      THEN CASE
        WHEN "ar1"."efft_d" = "ar1"."expy_d"
        THEN "ar1"."expy_d"
        WHEN "ar1"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "ar1"."expy_d"
      END
      ELSE CASE
        WHEN "ar1_2"."efft_d" = "ar1_2"."expy_d"
        THEN "ar1_2"."expy_d"
        WHEN "ar1_2"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "ar1_2"."expy_d"
      END
    END AS "expy_d"
  FROM "%%vtech%%"."acct_rel" AS "ar1"
  JOIN "%%vtech%%"."acct_rel" AS "ar1_2"
    ON "ar1"."subj_acct_i" = "ar1_2"."objc_acct_i"
    AND "ar1_2"."efft_d" <= :EXTR_D
    AND "ar1_2"."rel_c" = 'DITPS'
    AND (
      (
        "ar1"."efft_d" <= "ar1_2"."efft_d"
        AND "ar1_2"."efft_d" <= CASE
          WHEN "ar1"."efft_d" = "ar1"."expy_d"
          THEN "ar1"."expy_d"
          WHEN "ar1"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "ar1"."expy_d"
        END
      )
      OR (
        "ar1"."efft_d" <= CASE
          WHEN "ar1_2"."efft_d" = "ar1_2"."expy_d"
          THEN "ar1_2"."expy_d"
          WHEN "ar1_2"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "ar1_2"."expy_d"
        END
        AND "ar1"."efft_d" >= "ar1_2"."efft_d"
      )
    )
    AND CASE
      WHEN "ar1_2"."efft_d" = "ar1_2"."expy_d"
      THEN "ar1_2"."expy_d"
      WHEN "ar1_2"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "ar1_2"."expy_d"
    END >= :EXTR_D
  JOIN "%%ddstg%%"."acct_rel_wss_ditps" AS "ar3"
    ON "ar1_2"."objc_acct_i" = "ar3"."acct_i"
  WHERE
    "ar1"."efft_d" <= :EXTR_D
    AND "ar1"."rel_c" = 'FCLYO'
    AND (
      (
        "ar1"."efft_d" <= "ar1_2"."efft_d"
        AND "ar1_2"."efft_d" <= CASE
          WHEN "ar1"."efft_d" = "ar1"."expy_d"
          THEN "ar1"."expy_d"
          WHEN "ar1"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "ar1"."expy_d"
        END
      )
      OR (
        "ar1"."efft_d" <= CASE
          WHEN "ar1_2"."efft_d" = "ar1_2"."expy_d"
          THEN "ar1_2"."expy_d"
          WHEN "ar1_2"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "ar1_2"."expy_d"
        END
        AND "ar1"."efft_d" >= "ar1_2"."efft_d"
      )
    )
    AND CASE
      WHEN "ar1"."efft_d" = "ar1"."expy_d"
      THEN "ar1"."expy_d"
      WHEN "ar1"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "ar1"."expy_d"
    END >= :EXTR_D
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY "ar1_2"."objc_acct_i", "ar1_2"."rel_c"
      ORDER BY "ar1_2"."subj_acct_i" DESC NULLS LAST
    ) = 1
)
SELECT
  "rel"."acct_i" AS "acct_i",
  "acct_paty_dedup"."paty_i" AS "paty_i",
  "rel"."assc_acct_i" AS "assc_acct_i",
  "acct_paty_dedup"."paty_acct_rel_c" AS "paty_acct_rel_c",
  'N' AS "prfr_paty_f",
  "acct_paty_dedup"."srce_syst_c" AS "srce_syst_c",
  (
    CASE
      WHEN "acct_paty_dedup"."efft_d" > "rel"."efft_d"
      THEN "acct_paty_dedup"."efft_d"
      ELSE "rel"."efft_d"
    END
  ) AS "efft_d",
  (
    CASE
      WHEN CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END < "rel"."expy_d"
      THEN CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
      ELSE "rel"."expy_d"
    END
  ) AS "expy_d",
  "acct_paty_dedup"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "%%ddstg%%"."acct_paty_dedup" AS "acct_paty_dedup"
JOIN "rel" AS "rel"
  ON "acct_paty_dedup"."acct_i" = "rel"."assc_acct_i"
  AND (
    (
      "acct_paty_dedup"."efft_d" <= "rel"."efft_d"
      AND "rel"."efft_d" <= CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
    )
    OR (
      "acct_paty_dedup"."efft_d" <= "rel"."expy_d"
      AND "acct_paty_dedup"."efft_d" >= "rel"."efft_d"
    )
  )
WHERE
  "acct_paty_dedup"."efft_d" <= :EXTR_D
  AND (
    (
      "acct_paty_dedup"."efft_d" <= "rel"."efft_d"
      AND "rel"."efft_d" <= CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
    )
    OR (
      "acct_paty_dedup"."efft_d" <= "rel"."expy_d"
      AND "acct_paty_dedup"."efft_d" >= "rel"."efft_d"
    )
  )
  AND CASE
    WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
    THEN "acct_paty_dedup"."expy_d"
    WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
    THEN CAST('9999-12-31' AS DATE)
    ELSE "acct_paty_dedup"."expy_d"
  END >= :EXTR_D
GROUP BY
  "rel"."acct_i",
  "acct_paty_dedup"."paty_i",
  "rel"."assc_acct_i",
  "acct_paty_dedup"."paty_acct_rel_c",
  5,
  "acct_paty_dedup"."srce_syst_c",
  (
    CASE
      WHEN "acct_paty_dedup"."efft_d" > "rel"."efft_d"
      THEN "acct_paty_dedup"."efft_d"
      ELSE "rel"."efft_d"
    END
  ),
  (
    CASE
      WHEN CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END < "rel"."expy_d"
      THEN CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
      ELSE "rel"."expy_d"
    END
  ),
  "acct_paty_dedup"."row_secu_accs_c"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_PATY_DEDUP, ACCT_PATY_REL_WSS, ACCT_REL, ACCT_REL_WSS_DITPS
- **Columns**: ACCT_I, ASSC_ACCT_I, EFFT_D, EXPY_D, OBJC_ACCT_I, PATY_ACCT_REL_C, PATY_I, REL_C, ROW_SECU_ACCS_C, SRCE_SYST_C, SUBJ_ACCT_I
- **Functions**: '9999-12-31', (AP.EFFT_D BETWEEN REL.EFFT_D AND REL.EXPY_D), (DITPS.EFFT_D BETWEEN FCLYO.EFFT_D AND FCLYO.EXPY_D), AP.EFFT_D > REL.EFFT_D, AP.EXPY_D < REL.EXPY_D, EFFT_D = EXPY_D, EXPY_D >= :EXTR_D, FCLYO.EFFT_D > DITPS.EFFT_D, FCLYO.EXPY_D < DITPS.EXPY_D, None, REL_C = 'DITPS', REL_C = 'FCLYO'
- **Window_Functions**: None

### SQL Block 14 (Lines 768-778)
- **Complexity Score**: 27
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR
SELECT ACCT_I                        
,PATY_I                        
,ASSC_ACCT_I                   
,PATY_ACCT_REL_C               
,PRFR_PATY_F                   
,SRCE_SYST_C                   
,EFFT_D                        
,EXPY_D                        
,ROW_SECU_ACCS_C               
FROM %%DDSTG%%.ACCT_PATY_REL_WSS;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR SELECT ACCT_I, PATY_I, ASSC_ACCT_I, PATY_ACCT_REL_C, PRFR_PATY_F, SRCE_SYST_C, EFFT_D, EXPY_D, ROW_SECU_ACCS_C FROM %%DDSTG%%.ACCT_PATY_REL_WSS
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."derv_acct_paty_curr"
SELECT
  "acct_paty_rel_wss"."acct_i" AS "acct_i",
  "acct_paty_rel_wss"."paty_i" AS "paty_i",
  "acct_paty_rel_wss"."assc_acct_i" AS "assc_acct_i",
  "acct_paty_rel_wss"."paty_acct_rel_c" AS "paty_acct_rel_c",
  "acct_paty_rel_wss"."prfr_paty_f" AS "prfr_paty_f",
  "acct_paty_rel_wss"."srce_syst_c" AS "srce_syst_c",
  "acct_paty_rel_wss"."efft_d" AS "efft_d",
  "acct_paty_rel_wss"."expy_d" AS "expy_d",
  "acct_paty_rel_wss"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "%%ddstg%%"."acct_paty_rel_wss" AS "acct_paty_rel_wss"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_PATY_REL_WSS, DERV_ACCT_PATY_CURR
- **Columns**: ACCT_I, ASSC_ACCT_I, EFFT_D, EXPY_D, PATY_ACCT_REL_C, PATY_I, PRFR_PATY_F, ROW_SECU_ACCS_C, SRCE_SYST_C

### SQL Block 15 (Lines 801-818)
- **Complexity Score**: 61
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 3

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.ACCT_PATY_REL_THA
  
  -- first get the max  
    
     SEL THA_ACCT_I
          ,TRAD_ACCT_I
          ,EFFT_D
          ,CASE
                   WHEN EFFT_D = EXPY_D THEN EXPY_D
                   WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                    ELSE EXPY_D
              END AS EXPY_D
         
          FROM %%VTECH%%.THA_ACCT T1
          
       WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
          QUALIFY ROW_NUMBER() OVER (PARTITION BY THA_ACCT_I, EFFT_D
 ORDER BY TRAD_ACCT_I, CSL_CLNT_I DESC) = 1;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.ACCT_PATY_REL_THA SELECT THA_ACCT_I, TRAD_ACCT_I, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D FROM %%VTECH%%.THA_ACCT AS T1 WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D QUALIFY ROW_NUMBER() OVER (PARTITION BY THA_ACCT_I, EFFT_D ORDER BY TRAD_ACCT_I NULLS FIRST, CSL_CLNT_I DESC NULLS LAST) = 1
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- QUALIFY clause
- ROW_NUMBER() OVER
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."acct_paty_rel_tha"
SELECT
  "t1"."tha_acct_i" AS "tha_acct_i",
  "t1"."trad_acct_i" AS "trad_acct_i",
  "t1"."efft_d" AS "efft_d",
  CASE
    WHEN "t1"."efft_d" = "t1"."expy_d"
    THEN "t1"."expy_d"
    WHEN "t1"."expy_d" >= :EXTR_D
    THEN CAST('9999-12-31' AS DATE)
    ELSE "t1"."expy_d"
  END AS "expy_d"
FROM "%%vtech%%"."tha_acct" AS "t1"
WHERE
  "t1"."efft_d" <= :EXTR_D
  AND CASE
    WHEN "t1"."efft_d" = "t1"."expy_d"
    THEN "t1"."expy_d"
    WHEN "t1"."expy_d" >= :EXTR_D
    THEN CAST('9999-12-31' AS DATE)
    ELSE "t1"."expy_d"
  END >= :EXTR_D
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY "t1"."tha_acct_i", "t1"."efft_d"
    ORDER BY "t1"."trad_acct_i" NULLS FIRST, "t1"."csl_clnt_i" DESC NULLS LAST
  ) = 1
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_PATY_REL_THA, THA_ACCT
- **Columns**: CSL_CLNT_I, EFFT_D, EXPY_D, THA_ACCT_I, TRAD_ACCT_I
- **Functions**: '9999-12-31', EFFT_D = EXPY_D, EXPY_D >= :EXTR_D, None
- **Window_Functions**: None

### SQL Block 16 (Lines 830-854)
- **Complexity Score**: 75
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE
SEL THA_ACCT_I
     ,TRAD_ACCT_I
      ,EFFT_D
     ,EXPY_D
     ,CASE WHEN NEW_EXPY_D IS NULL THEN EXPY_D 
           WHEN NEW_EXPY_D <= EXPY_D AND NEW_EXPY_D > EFFT_D THEN NEW_EXPY_D - 1     
           ELSE EXPY_D
       END AS NEW_EXPY_D 
       FROM

(SELECT THA_ACCT_I
             ,TRAD_ACCT_I
             ,EFFT_D
             ,EXPY_D
             ,MIN(EFFT_D)  
             OVER(PARTITION BY THA_ACCT_I
                         ORDER BY EFFT_D, EXPY_D
                         ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
                        ) AS NEW_EXPY_D
      FROM %%DDSTG%%.ACCT_PATY_REL_THA
   
     
    ) T;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE SELECT THA_ACCT_I, TRAD_ACCT_I, EFFT_D, EXPY_D, CASE WHEN NEW_EXPY_D IS NULL THEN EXPY_D WHEN NEW_EXPY_D <= EXPY_D AND NEW_EXPY_D > EFFT_D THEN NEW_EXPY_D - 1 ELSE EXPY_D END AS NEW_EXPY_D FROM (SELECT THA_ACCT_I, TRAD_ACCT_I, EFFT_D, EXPY_D, MIN(EFFT_D) OVER (PARTITION BY THA_ACCT_I ORDER BY EFFT_D NULLS FIRST, EXPY_D NULLS FIRST ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS NEW_EXPY_D FROM %%DDSTG%%.ACCT_PATY_REL_THA) AS T
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."acct_paty_tha_new_rnge"
WITH "t" AS (
  SELECT
    "acct_paty_rel_tha"."tha_acct_i" AS "tha_acct_i",
    "acct_paty_rel_tha"."trad_acct_i" AS "trad_acct_i",
    "acct_paty_rel_tha"."efft_d" AS "efft_d",
    "acct_paty_rel_tha"."expy_d" AS "expy_d",
    MIN("acct_paty_rel_tha"."efft_d") OVER (
      PARTITION BY "acct_paty_rel_tha"."tha_acct_i"
      ORDER BY "acct_paty_rel_tha"."efft_d" NULLS FIRST, "acct_paty_rel_tha"."expy_d" NULLS FIRST
      ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
    ) AS "new_expy_d"
  FROM "%%ddstg%%"."acct_paty_rel_tha" AS "acct_paty_rel_tha"
)
SELECT
  "t"."tha_acct_i" AS "tha_acct_i",
  "t"."trad_acct_i" AS "trad_acct_i",
  "t"."efft_d" AS "efft_d",
  "t"."expy_d" AS "expy_d",
  CASE
    WHEN "t"."new_expy_d" IS NULL
    THEN "t"."expy_d"
    WHEN "t"."efft_d" < "t"."new_expy_d" AND "t"."expy_d" >= "t"."new_expy_d"
    THEN "t"."new_expy_d" - 1
    ELSE "t"."expy_d"
  END AS "new_expy_d"
FROM "t" AS "t"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_PATY_REL_THA, ACCT_PATY_THA_NEW_RNGE
- **Columns**: EFFT_D, EXPY_D, NEW_EXPY_D, THA_ACCT_I, TRAD_ACCT_I
- **Functions**: EFFT_D, NEW_EXPY_D <= EXPY_D, NEW_EXPY_D <= EXPY_D AND NEW_EXPY_D > EFFT_D, NEW_EXPY_D IS NULL, None
- **Window_Functions**: EFFT_D

### SQL Block 17 (Lines 860-876)
- **Complexity Score**: 81
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE
     SEL T1.THA_ACCT_I
     ,T1.TRAD_ACCT_I
     ,T1.NEW_EXPY_D + 1 AS EFFT_D
    ,T1.EXPY_D
    ,T1.EXPY_D
    FROM %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE T1
    LEFT JOIN %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE T2
    
    ON T1.THA_ACCT_I = T2.THA_ACCT_I
    
    AND T1.NEW_EXPY_D + 1  = T2.EFFT_D
    WHERE T1.NEW_EXPY_D < T1.EXPY_D AND T1.NEW_EXPY_D > T1.EFFT_D
    AND T2.THA_ACCT_I IS NULL
    
    GROUP BY 1,2,3,4,5;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE SELECT T1.THA_ACCT_I, T1.TRAD_ACCT_I, T1.NEW_EXPY_D + 1 AS EFFT_D, T1.EXPY_D, T1.EXPY_D FROM %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE AS T1 LEFT JOIN %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE AS T2 ON T1.THA_ACCT_I = T2.THA_ACCT_I AND T1.NEW_EXPY_D + 1 = T2.EFFT_D WHERE T1.NEW_EXPY_D < T1.EXPY_D AND T1.NEW_EXPY_D > T1.EFFT_D AND T2.THA_ACCT_I IS NULL GROUP BY 1, 2, 3, 4, 5
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."acct_paty_tha_new_rnge"
SELECT
  "t1"."tha_acct_i" AS "tha_acct_i",
  "t1"."trad_acct_i" AS "trad_acct_i",
  "t1"."new_expy_d" + 1 AS "efft_d",
  "t1"."expy_d" AS "expy_d",
  "t1"."expy_d" AS "expy_d"
FROM "%%ddstg%%"."acct_paty_tha_new_rnge" AS "t1"
LEFT JOIN "%%ddstg%%"."acct_paty_tha_new_rnge" AS "t2"
  ON "t1"."tha_acct_i" = "t2"."tha_acct_i" AND "t2"."efft_d" = "t1"."new_expy_d" + 1
WHERE
  "t1"."efft_d" < "t1"."new_expy_d"
  AND "t1"."expy_d" > "t1"."new_expy_d"
  AND "t2"."tha_acct_i" IS NULL
GROUP BY
  "t1"."tha_acct_i",
  "t1"."trad_acct_i",
  "t1"."new_expy_d" + 1,
  "t1"."expy_d",
  "t1"."expy_d"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_PATY_THA_NEW_RNGE
- **Columns**: EFFT_D, EXPY_D, NEW_EXPY_D, THA_ACCT_I, TRAD_ACCT_I
- **Functions**: T1.NEW_EXPY_D < T1.EXPY_D, T1.NEW_EXPY_D < T1.EXPY_D AND T1.NEW_EXPY_D > T1.EFFT_D, T1.THA_ACCT_I = T2.THA_ACCT_I

### SQL Block 18 (Lines 890-899)
- **Complexity Score**: 22
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.ACCT_PATY_REL_THA
    
    SEL THA_ACCT_I
          ,MAX(TRAD_ACCT_I)
          ,EFFT_D
          ,NEW_EXPY_D
          FROM  %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE
 
          GROUP BY 1,3,4;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.ACCT_PATY_REL_THA SELECT THA_ACCT_I, MAX(TRAD_ACCT_I), EFFT_D, NEW_EXPY_D FROM %%DDSTG%%.ACCT_PATY_THA_NEW_RNGE GROUP BY 1, 3, 4
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."acct_paty_rel_tha"
SELECT
  "acct_paty_tha_new_rnge"."tha_acct_i" AS "tha_acct_i",
  MAX("acct_paty_tha_new_rnge"."trad_acct_i") AS "_col_1",
  "acct_paty_tha_new_rnge"."efft_d" AS "efft_d",
  "acct_paty_tha_new_rnge"."new_expy_d" AS "new_expy_d"
FROM "%%ddstg%%"."acct_paty_tha_new_rnge" AS "acct_paty_tha_new_rnge"
GROUP BY
  "acct_paty_tha_new_rnge"."tha_acct_i",
  "acct_paty_tha_new_rnge"."efft_d",
  "acct_paty_tha_new_rnge"."new_expy_d"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_PATY_REL_THA, ACCT_PATY_THA_NEW_RNGE
- **Columns**: EFFT_D, NEW_EXPY_D, THA_ACCT_I, TRAD_ACCT_I
- **Functions**: TRAD_ACCT_I

### SQL Block 19 (Lines 910-957)
- **Complexity Score**: 186
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR
SELECT ACCT_I
            , PATY_I
            , MAX(ASSC_ACCT_I)
            , PATY_aCCT_REL_C
            , PRFR_PATY_F
            , SRCE_SYST_C
            , EFFT_D
            , EXPY_D
             ,ROW_SECU_ACCS_C
            FROM (

SELECT TA.THA_ACCT_I AS ACCT_I
, AP.PATY_I
,TA.TRAD_ACCT_I AS ASSC_ACCT_I
, AP.PATY_ACCT_REL_C
,'N' AS PRFR_PATY_F
, AP.SRCE_SYST_C
, (CASE WHEN TA.EFFT_D >  AP.EFFT_D  THEN TA.EFFT_D  ELSE AP.EFFT_D END) AS EFFT_D
, (CASE WHEN TA.EXPY_D <  AP.EXPY_D THEN TA.EXPY_D ELSE AP.EXPY_D END) AS EXPY_D
 ,AP.ROW_SECU_ACCS_C
 FROM   (
                SEL ACCT_I
                     ,PATY_I
                     ,PATY_ACCT_REL_C
                     ,SRCE_SYST_C
                     ,EFFT_D
                     ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                                 ELSE EXPY_D
                        END AS EXPY_D
                     ,ROW_SECU_ACCS_C       
           FROM    %%DDSTG%%.ACCT_PATY_DEDUP
             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP

    JOIN %%DDSTG%%.ACCT_PATY_REL_THA TA
      ON TA.TRAD_ACCT_I = AP.ACCT_I 
  
 WHERE ( 
        (TA.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D) 
    OR  (AP.EFFT_D BETWEEN TA.EFFT_D AND TA.EXPY_D) 
        ) 
  
  
     ) T 
        
GROUP BY 1,2,4,5,6,7,8,9;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR SELECT ACCT_I, PATY_I, MAX(ASSC_ACCT_I), PATY_aCCT_REL_C, PRFR_PATY_F, SRCE_SYST_C, EFFT_D, EXPY_D, ROW_SECU_ACCS_C FROM (SELECT TA.THA_ACCT_I AS ACCT_I, AP.PATY_I, TA.TRAD_ACCT_I AS ASSC_ACCT_I, AP.PATY_ACCT_REL_C, 'N' AS PRFR_PATY_F, AP.SRCE_SYST_C, (CASE WHEN TA.EFFT_D > AP.EFFT_D THEN TA.EFFT_D ELSE AP.EFFT_D END) AS EFFT_D, (CASE WHEN TA.EXPY_D < AP.EXPY_D THEN TA.EXPY_D ELSE AP.EXPY_D END) AS EXPY_D, AP.ROW_SECU_ACCS_C FROM (SELECT ACCT_I, PATY_I, PATY_ACCT_REL_C, SRCE_SYST_C, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D, ROW_SECU_ACCS_C FROM %%DDSTG%%.ACCT_PATY_DEDUP WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS AP JOIN %%DDSTG%%.ACCT_PATY_REL_THA AS TA ON TA.TRAD_ACCT_I = AP.ACCT_I WHERE ((TA.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D) OR (AP.EFFT_D BETWEEN TA.EFFT_D AND TA.EXPY_D))) AS T GROUP BY 1, 2, 4, 5, 6, 7, 8, 9
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."derv_acct_paty_curr"
SELECT
  "ta"."tha_acct_i" AS "acct_i",
  "acct_paty_dedup"."paty_i" AS "paty_i",
  MAX("ta"."trad_acct_i") AS "_col_2",
  "acct_paty_dedup"."paty_acct_rel_c" AS "paty_acct_rel_c",
  'N' AS "prfr_paty_f",
  "acct_paty_dedup"."srce_syst_c" AS "srce_syst_c",
  (
    CASE
      WHEN "ta"."efft_d" > "acct_paty_dedup"."efft_d"
      THEN "ta"."efft_d"
      ELSE "acct_paty_dedup"."efft_d"
    END
  ) AS "efft_d",
  (
    CASE
      WHEN "ta"."expy_d" < CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
      THEN "ta"."expy_d"
      ELSE CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
    END
  ) AS "expy_d",
  "acct_paty_dedup"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "%%ddstg%%"."acct_paty_dedup" AS "acct_paty_dedup"
JOIN "%%ddstg%%"."acct_paty_rel_tha" AS "ta"
  ON "acct_paty_dedup"."acct_i" = "ta"."trad_acct_i"
  AND (
    (
      "acct_paty_dedup"."efft_d" <= "ta"."efft_d"
      AND "ta"."efft_d" <= CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
    )
    OR (
      "acct_paty_dedup"."efft_d" <= "ta"."expy_d"
      AND "acct_paty_dedup"."efft_d" >= "ta"."efft_d"
    )
  )
WHERE
  "acct_paty_dedup"."efft_d" <= :EXTR_D
  AND (
    (
      "acct_paty_dedup"."efft_d" <= "ta"."efft_d"
      AND "ta"."efft_d" <= CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
    )
    OR (
      "acct_paty_dedup"."efft_d" <= "ta"."expy_d"
      AND "acct_paty_dedup"."efft_d" >= "ta"."efft_d"
    )
  )
  AND CASE
    WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
    THEN "acct_paty_dedup"."expy_d"
    WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
    THEN CAST('9999-12-31' AS DATE)
    ELSE "acct_paty_dedup"."expy_d"
  END >= :EXTR_D
GROUP BY
  "ta"."tha_acct_i",
  "acct_paty_dedup"."paty_i",
  "acct_paty_dedup"."paty_acct_rel_c",
  'N',
  "acct_paty_dedup"."srce_syst_c",
  (
    CASE
      WHEN "ta"."efft_d" > "acct_paty_dedup"."efft_d"
      THEN "ta"."efft_d"
      ELSE "acct_paty_dedup"."efft_d"
    END
  ),
  (
    CASE
      WHEN "ta"."expy_d" < CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
      THEN "ta"."expy_d"
      ELSE CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
    END
  ),
  "acct_paty_dedup"."row_secu_accs_c"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_PATY_DEDUP, ACCT_PATY_REL_THA, DERV_ACCT_PATY_CURR
- **Columns**: ACCT_I, ASSC_ACCT_I, EFFT_D, EXPY_D, PATY_ACCT_REL_C, PATY_I, PATY_aCCT_REL_C, PRFR_PATY_F, ROW_SECU_ACCS_C, SRCE_SYST_C, THA_ACCT_I, TRAD_ACCT_I
- **Functions**: '9999-12-31', (TA.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D), ASSC_ACCT_I, EFFT_D = EXPY_D, EXPY_D >= :EXTR_D, None, TA.EFFT_D > AP.EFFT_D, TA.EXPY_D < AP.EXPY_D

### SQL Block 20 (Lines 976-1057)
- **Complexity Score**: 365
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR
SELECT XREF.ACCT_I 
      ,AP.PATY_I
      ,XREF.ASSC_ACCT_I 
     ,AP.PATY_ACCT_REL_C
     ,'N' AS PRFR_PATY_F
     ,AP.SRCE_SYST_C
    
   ,(CASE WHEN AP.EFFT_D > XREF.EFFT_D THEN AP.EFFT_D ELSE XREF.EFFT_D END) AS EFFT_D
   ,(CASE WHEN AP.EXPY_D < XREF.EXPY_D THEN AP.EXPY_D ELSE XREF.EXPY_D END) AS EXPY_D
   ,AP.ROW_SECU_ACCS_C
FROM (
                SEL ACCT_I
                     ,PATY_I
                     ,PATY_ACCT_REL_C
                     ,SRCE_SYST_C
                     ,EFFT_D
                     ,CASE
                               WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                                 ELSE EXPY_D
                        END AS EXPY_D
                     ,ROW_SECU_ACCS_C       
           FROM    %%DDSTG%%.ACCT_PATY_DEDUP
             WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  AP
JOIN (
SEL XREF1.ACCT_I   
    ,XREF2. ASSC_ACCT_I
    ,(CASE WHEN XREF2.EFFT_D > XREF1.EFFT_D THEN XREF2.EFFT_D ELSE XREF1.EFFT_D END) AS EFFT_D
    ,(CASE WHEN XREF2.EXPY_D < XREF1.EXPY_D THEN XREF2.EXPY_D ELSE XREF1.EXPY_D END) AS EXPY_D

FROM (SEL ACCT_I
                     ,SRCE_SYST_PATY_I
                     ,SRCE_SYST_C
                     ,EFFT_D
                     ,CASE
                                WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                                 ELSE EXPY_D
                        END AS EXPY_D

           FROM   %%VTECH%%.ACCT_UNID_PATY
           WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)   XREF1
JOIN (SEL ACCT_I AS ASSC_ACCT_I 
                  ,SRCE_SYST_PATY_I
                  ,PATY_ACCT_REL_C
                  ,SRCE_SYST_C
                  ,ORIG_SRCE_SYST_C         -- R32 change
                  ,EFFT_D
                     ,CASE
                               WHEN EFFT_D = EXPY_D THEN EXPY_D
                                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                                 ELSE EXPY_D
                        END AS EXPY_D

           FROM   %%VTECH%%.ACCT_UNID_PATY
           WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D)  XREF2 
  ON XREF2.SRCE_SYST_PATY_I = XREF1.SRCE_SYST_PATY_I 
  
JOIN %%DDSTG%%.GRD_GNRC_MAP_DERV_UNID_PATY GGM 
  ON GGM.UNID_PATY_SRCE_SYST_C = XREF2.SRCE_SYST_C
 AND GGM.UNID_PATY_ACCT_REL_C  = XREF2.PATY_ACCT_REL_C
  
   -- R32 change starts
 AND GGM.SRCE_SYST_C  = XREF2.ORIG_SRCE_SYST_C 
  -- R32 change ends
  
WHERE XREF1.SRCE_SYST_C = GGM.SRCE_SYST_C
AND  (
      (XREF1.EFFT_D BETWEEN XREF2.EFFT_D AND XREF2.EXPY_D) 
  OR  (XREF2.EFFT_D BETWEEN XREF1.EFFT_D AND XREF1.EXPY_D))
     ) XREF  
     
  ON XREF.ASSC_ACCT_I = AP.ACCT_I
WHERE  ((AP.EFFT_D BETWEEN XREF.EFFT_D AND XREF.EXPY_D) 
    OR  (XREF.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)
    )



GROUP BY 1,2,3,4,5,6,7,8,9;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR SELECT XREF.ACCT_I, AP.PATY_I, XREF.ASSC_ACCT_I, AP.PATY_ACCT_REL_C, 'N' AS PRFR_PATY_F, AP.SRCE_SYST_C, (CASE WHEN AP.EFFT_D > XREF.EFFT_D THEN AP.EFFT_D ELSE XREF.EFFT_D END) AS EFFT_D, (CASE WHEN AP.EXPY_D < XREF.EXPY_D THEN AP.EXPY_D ELSE XREF.EXPY_D END) AS EXPY_D, AP.ROW_SECU_ACCS_C FROM (SELECT ACCT_I, PATY_I, PATY_ACCT_REL_C, SRCE_SYST_C, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D, ROW_SECU_ACCS_C FROM %%DDSTG%%.ACCT_PATY_DEDUP WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS AP JOIN (SELECT XREF1.ACCT_I, XREF2.ASSC_ACCT_I, (CASE WHEN XREF2.EFFT_D > XREF1.EFFT_D THEN XREF2.EFFT_D ELSE XREF1.EFFT_D END) AS EFFT_D, (CASE WHEN XREF2.EXPY_D < XREF1.EXPY_D THEN XREF2.EXPY_D ELSE XREF1.EXPY_D END) AS EXPY_D FROM (SELECT ACCT_I, SRCE_SYST_PATY_I, SRCE_SYST_C, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D FROM %%VTECH%%.ACCT_UNID_PATY WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS XREF1 JOIN (SELECT ACCT_I AS ASSC_ACCT_I, SRCE_SYST_PATY_I, PATY_ACCT_REL_C, SRCE_SYST_C, ORIG_SRCE_SYST_C, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D FROM %%VTECH%%.ACCT_UNID_PATY WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS XREF2 ON XREF2.SRCE_SYST_PATY_I = XREF1.SRCE_SYST_PATY_I JOIN %%DDSTG%%.GRD_GNRC_MAP_DERV_UNID_PATY AS GGM ON GGM.UNID_PATY_SRCE_SYST_C = XREF2.SRCE_SYST_C AND GGM.UNID_PATY_ACCT_REL_C = XREF2.PATY_ACCT_REL_C AND GGM.SRCE_SYST_C = XREF2.ORIG_SRCE_SYST_C WHERE XREF1.SRCE_SYST_C = GGM.SRCE_SYST_C AND ((XREF1.EFFT_D BETWEEN XREF2.EFFT_D AND XREF2.EXPY_D) OR (XREF2.EFFT_D BETWEEN XREF1.EFFT_D AND XREF1.EXPY_D))) AS XREF ON XREF.ASSC_ACCT_I = AP.ACCT_I WHERE ((AP.EFFT_D BETWEEN XREF.EFFT_D AND XREF.EXPY_D) OR (XREF.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D)) GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."derv_acct_paty_curr"
WITH "xref" AS (
  SELECT
    "acct_unid_paty"."acct_i" AS "acct_i",
    "acct_unid_paty_2"."acct_i" AS "assc_acct_i",
    CASE
      WHEN "acct_unid_paty"."efft_d" < "acct_unid_paty_2"."efft_d"
      THEN "acct_unid_paty_2"."efft_d"
      ELSE "acct_unid_paty"."efft_d"
    END AS "efft_d",
    CASE
      WHEN CASE
        WHEN "acct_unid_paty"."efft_d" = "acct_unid_paty"."expy_d"
        THEN "acct_unid_paty"."expy_d"
        WHEN "acct_unid_paty"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_unid_paty"."expy_d"
      END > CASE
        WHEN "acct_unid_paty_2"."efft_d" = "acct_unid_paty_2"."expy_d"
        THEN "acct_unid_paty_2"."expy_d"
        WHEN "acct_unid_paty_2"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_unid_paty_2"."expy_d"
      END
      THEN CASE
        WHEN "acct_unid_paty_2"."efft_d" = "acct_unid_paty_2"."expy_d"
        THEN "acct_unid_paty_2"."expy_d"
        WHEN "acct_unid_paty_2"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_unid_paty_2"."expy_d"
      END
      ELSE CASE
        WHEN "acct_unid_paty"."efft_d" = "acct_unid_paty"."expy_d"
        THEN "acct_unid_paty"."expy_d"
        WHEN "acct_unid_paty"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_unid_paty"."expy_d"
      END
    END AS "expy_d"
  FROM "%%vtech%%"."acct_unid_paty" AS "acct_unid_paty"
  JOIN "%%vtech%%"."acct_unid_paty" AS "acct_unid_paty_2"
    ON "acct_unid_paty"."srce_syst_paty_i" = "acct_unid_paty_2"."srce_syst_paty_i"
    AND "acct_unid_paty_2"."efft_d" <= :EXTR_D
    AND (
      (
        "acct_unid_paty"."efft_d" <= "acct_unid_paty_2"."efft_d"
        AND "acct_unid_paty_2"."efft_d" <= CASE
          WHEN "acct_unid_paty"."efft_d" = "acct_unid_paty"."expy_d"
          THEN "acct_unid_paty"."expy_d"
          WHEN "acct_unid_paty"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "acct_unid_paty"."expy_d"
        END
      )
      OR (
        "acct_unid_paty"."efft_d" <= CASE
          WHEN "acct_unid_paty_2"."efft_d" = "acct_unid_paty_2"."expy_d"
          THEN "acct_unid_paty_2"."expy_d"
          WHEN "acct_unid_paty_2"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "acct_unid_paty_2"."expy_d"
        END
        AND "acct_unid_paty"."efft_d" >= "acct_unid_paty_2"."efft_d"
      )
    )
    AND CASE
      WHEN "acct_unid_paty_2"."efft_d" = "acct_unid_paty_2"."expy_d"
      THEN "acct_unid_paty_2"."expy_d"
      WHEN "acct_unid_paty_2"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "acct_unid_paty_2"."expy_d"
    END >= :EXTR_D
  JOIN "%%ddstg%%"."grd_gnrc_map_derv_unid_paty" AS "ggm"
    ON "acct_unid_paty"."srce_syst_c" = "ggm"."srce_syst_c"
    AND "acct_unid_paty_2"."orig_srce_syst_c" = "ggm"."srce_syst_c"
    AND "acct_unid_paty_2"."paty_acct_rel_c" = "ggm"."unid_paty_acct_rel_c"
    AND "acct_unid_paty_2"."srce_syst_c" = "ggm"."unid_paty_srce_syst_c"
  WHERE
    "acct_unid_paty"."efft_d" <= :EXTR_D
    AND CASE
      WHEN "acct_unid_paty"."efft_d" = "acct_unid_paty"."expy_d"
      THEN "acct_unid_paty"."expy_d"
      WHEN "acct_unid_paty"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "acct_unid_paty"."expy_d"
    END >= :EXTR_D
)
SELECT
  "xref"."acct_i" AS "acct_i",
  "acct_paty_dedup"."paty_i" AS "paty_i",
  "xref"."assc_acct_i" AS "assc_acct_i",
  "acct_paty_dedup"."paty_acct_rel_c" AS "paty_acct_rel_c",
  'N' AS "prfr_paty_f",
  "acct_paty_dedup"."srce_syst_c" AS "srce_syst_c",
  (
    CASE
      WHEN "acct_paty_dedup"."efft_d" > "xref"."efft_d"
      THEN "acct_paty_dedup"."efft_d"
      ELSE "xref"."efft_d"
    END
  ) AS "efft_d",
  (
    CASE
      WHEN CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END < "xref"."expy_d"
      THEN CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
      ELSE "xref"."expy_d"
    END
  ) AS "expy_d",
  "acct_paty_dedup"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "%%ddstg%%"."acct_paty_dedup" AS "acct_paty_dedup"
JOIN "xref" AS "xref"
  ON "acct_paty_dedup"."acct_i" = "xref"."assc_acct_i"
  AND (
    (
      "acct_paty_dedup"."efft_d" <= "xref"."efft_d"
      AND "xref"."efft_d" <= CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
    )
    OR (
      "acct_paty_dedup"."efft_d" <= "xref"."expy_d"
      AND "acct_paty_dedup"."efft_d" >= "xref"."efft_d"
    )
  )
WHERE
  "acct_paty_dedup"."efft_d" <= :EXTR_D
  AND (
    (
      "acct_paty_dedup"."efft_d" <= "xref"."efft_d"
      AND "xref"."efft_d" <= CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
    )
    OR (
      "acct_paty_dedup"."efft_d" <= "xref"."expy_d"
      AND "acct_paty_dedup"."efft_d" >= "xref"."efft_d"
    )
  )
  AND CASE
    WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
    THEN "acct_paty_dedup"."expy_d"
    WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
    THEN CAST('9999-12-31' AS DATE)
    ELSE "acct_paty_dedup"."expy_d"
  END >= :EXTR_D
GROUP BY
  "xref"."acct_i",
  "acct_paty_dedup"."paty_i",
  "xref"."assc_acct_i",
  "acct_paty_dedup"."paty_acct_rel_c",
  5,
  "acct_paty_dedup"."srce_syst_c",
  (
    CASE
      WHEN "acct_paty_dedup"."efft_d" > "xref"."efft_d"
      THEN "acct_paty_dedup"."efft_d"
      ELSE "xref"."efft_d"
    END
  ),
  (
    CASE
      WHEN CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END < "xref"."expy_d"
      THEN CASE
        WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
        THEN "acct_paty_dedup"."expy_d"
        WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "acct_paty_dedup"."expy_d"
      END
      ELSE "xref"."expy_d"
    END
  ),
  "acct_paty_dedup"."row_secu_accs_c"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_PATY_DEDUP, ACCT_UNID_PATY, DERV_ACCT_PATY_CURR, GRD_GNRC_MAP_DERV_UNID_PATY
- **Columns**: ACCT_I, ASSC_ACCT_I, EFFT_D, EXPY_D, ORIG_SRCE_SYST_C, PATY_ACCT_REL_C, PATY_I, ROW_SECU_ACCS_C, SRCE_SYST_C, SRCE_SYST_PATY_I, UNID_PATY_ACCT_REL_C, UNID_PATY_SRCE_SYST_C
- **Functions**: '9999-12-31', (AP.EFFT_D BETWEEN XREF.EFFT_D AND XREF.EXPY_D), (XREF1.EFFT_D BETWEEN XREF2.EFFT_D AND XREF2.EXPY_D), AP.EFFT_D > XREF.EFFT_D, AP.EXPY_D < XREF.EXPY_D, EFFT_D = EXPY_D, EXPY_D >= :EXTR_D, GGM.UNID_PATY_SRCE_SYST_C = XREF2.SRCE_SYST_C, GGM.UNID_PATY_SRCE_SYST_C = XREF2.SRCE_SYST_C AND GGM.UNID_PATY_ACCT_REL_C = XREF2.PATY_ACCT_REL_C, None, XREF1.SRCE_SYST_C = GGM.SRCE_SYST_C, XREF2.EFFT_D > XREF1.EFFT_D, XREF2.EXPY_D < XREF1.EXPY_D

### SQL Block 21 (Lines 1077-1241)
- **Complexity Score**: 788
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR
SELECT AX.ACCT_I
       ,AP.PATY_I
       ,AX.ASSC_ACCT_I 
       ,AP.PATY_ACCT_REL_C
       ,'N' AS PRFR_PATY_F  
       ,AP.SRCE_SYST_C
       ,(CASE WHEN AP.EFFT_D > AX.EFFT_D THEN AP.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D
       ,(CASE WHEN AP.EXPY_D < AX.EXPY_D THEN AP.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D
       ,AP.ROW_SECU_ACCS_C
FROM (SEL ACCT_I
          ,PATY_I
          ,PATY_ACCT_REL_C
          ,SRCE_SYST_C
           ,EFFT_D
           ,CASE
               WHEN EFFT_D = EXPY_D THEN EXPY_D
               WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
               ELSE EXPY_D
            END AS EXPY_D
            ,ROW_SECU_ACCS_C       
           FROM %%DDSTG%%.ACCT_PATY_DEDUP
       WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AP
JOIN
( --start AX
SELECT T1.LOAN_I AS ACCT_I
       ,UIP.ACCT_I AS ASSC_ACCT_I
        ,(CASE WHEN UIP.EFFT_D > T1.EFFT_D THEN UIP.EFFT_D ELSE T1.EFFT_D END) AS EFFT_D
       ,(CASE WHEN UIP.EXPY_D < T1.EXPY_D THEN UIP.EXPY_D ELSE T1.EXPY_D END) AS EXPY_D
       FROM    
(  --start T1
SELECT LOAN.LOAN_I 
       ,FCLY.SRCE_SYST_PATY_I
       ,(CASE WHEN LOAN.EFFT_D > FCLY.EFFT_D THEN LOAN.EFFT_D ELSE FCLY.EFFT_D END) AS EFFT_D
       ,(CASE WHEN LOAN.EXPY_D < FCLY.EXPY_D THEN LOAN.EXPY_D ELSE FCLY.EXPY_D END) AS EXPY_D 
FROM ( SEL LOAN_I
           ,FCLY_I
           ,EFFT_D
            ,CASE
                WHEN EFFT_D = EXPY_D THEN EXPY_D
                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                ELSE EXPY_D
             END AS EXPY_D
      FROM %%VTECH%%.MOS_LOAN 
      WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) LOAN 
JOIN (SEL SUBSTR(FCLY_I,1,14) AS MOS_FCLY_I
         ,SRCE_SYST_PATY_I
         ,EFFT_D
           ,CASE
                WHEN EFFT_D = EXPY_D THEN EXPY_D
                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                ELSE EXPY_D
             END AS EXPY_D
         FROM %%VTECH%%.MOS_FCLY
        WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) FCLY  
           ON FCLY.MOS_FCLY_I = LOAN.FCLY_I
       WHERE (
              (FCLY.EFFT_D BETWEEN LOAN.EFFT_D AND LOAN.EXPY_D) 
             OR (LOAN.EFFT_D BETWEEN FCLY.EFFT_D AND FCLY.EXPY_D)
            )
 ) T1           
JOIN (SELECT ACCT_I 
               ,SRCE_SYST_PATY_I
               ,EFFT_D
               ,CASE
                  WHEN EFFT_D = EXPY_D THEN EXPY_D
                  WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                  ELSE EXPY_D
                END AS EXPY_D
           FROM %%VTECH%%.ACCT_UNID_PATY
           WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
            AND SRCE_SYST_C = 'SAP' 
            AND PATY_ACCT_REL_C = 'ACTO') UIP 
ON UIP.SRCE_SYST_PATY_I = T1.SRCE_SYST_PATY_I 
WHERE (
       (UIP.EFFT_D BETWEEN T1.EFFT_D AND T1.EXPY_D) 
        OR (T1.EFFT_D BETWEEN UIP.EFFT_D AND UIP.EXPY_D)
      )
) AX  
ON AX.ASSC_ACCT_I = AP.ACCT_I

WHERE (
       (AX.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D) 
        OR (AP.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D)
      )



GROUP BY 1,2,3,4,5,6,7,8,9

UNION ALL



-- Generate rows for facility accounts

SELECT  AX.ACCT_I
       ,AP.PATY_I
       ,AX.ASSC_ACCT_I 
       ,AP.PATY_ACCT_REL_C
       ,'N' AS PRFR_PATY_F  
       ,AP.SRCE_SYST_C
       ,(CASE WHEN AP.EFFT_D > AX.EFFT_D THEN AP.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D
       ,(CASE WHEN AP.EXPY_D < AX.EXPY_D THEN AP.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D
       ,AP.ROW_SECU_ACCS_C
FROM (SEL ACCT_I
         ,PATY_I
         ,PATY_ACCT_REL_C
         ,SRCE_SYST_C
         ,EFFT_D
         ,CASE
              WHEN EFFT_D = EXPY_D THEN EXPY_D
              WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
               ELSE EXPY_D
          END AS EXPY_D
          ,ROW_SECU_ACCS_C       
       FROM %%DDSTG%%.ACCT_PATY_DEDUP
       WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AP
JOIN 
(--start AX
SELECT FCLY.FCLY_I AS ACCT_I 
       ,UIP.ACCT_I AS ASSC_ACCT_I
       ,(CASE WHEN UIP.EFFT_D > FCLY.EFFT_D THEN UIP.EFFT_D ELSE FCLY.EFFT_D END) AS EFFT_D
       ,(CASE WHEN UIP.EXPY_D < FCLY.EXPY_D THEN UIP.EXPY_D ELSE FCLY.EXPY_D END) AS EXPY_D
FROM (SEL FCLY_I
         ,SRCE_SYST_PATY_I
         ,EFFT_D
           ,CASE
                WHEN EFFT_D = EXPY_D THEN EXPY_D
                WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                ELSE EXPY_D
             END AS EXPY_D
         FROM %%VTECH%%.MOS_FCLY
        WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) FCLY  
JOIN (SELECT ACCT_I 
               ,SRCE_SYST_PATY_I
               ,EFFT_D
               ,CASE
                  WHEN EFFT_D = EXPY_D THEN EXPY_D
                  WHEN EXPY_D >= :EXTR_D THEN DATE'9999-12-31'
                  ELSE EXPY_D
                END AS EXPY_D
           FROM %%VTECH%%.ACCT_UNID_PATY
           WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D
            AND SRCE_SYST_C = 'SAP' 
            AND PATY_ACCT_REL_C = 'ACTO') UIP 
ON UIP.SRCE_SYST_PATY_I = FCLY.SRCE_SYST_PATY_I 
WHERE (
       (UIP.EFFT_D BETWEEN FCLY.EFFT_D AND FCLY.EXPY_D) 
        OR (FCLY.EFFT_D BETWEEN UIP.EFFT_D AND UIP.EXPY_D)
      )
 
  
) AX 

ON AX.ASSC_ACCT_I = AP.ACCT_I

WHERE (
       (AX.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D) 
        OR (AP.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D)
      )
 

GROUP BY 1,2,3,4,5,6,7,8,9     
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_CURR SELECT AX.ACCT_I, AP.PATY_I, AX.ASSC_ACCT_I, AP.PATY_ACCT_REL_C, 'N' AS PRFR_PATY_F, AP.SRCE_SYST_C, (CASE WHEN AP.EFFT_D > AX.EFFT_D THEN AP.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D, (CASE WHEN AP.EXPY_D < AX.EXPY_D THEN AP.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D, AP.ROW_SECU_ACCS_C FROM (SELECT ACCT_I, PATY_I, PATY_ACCT_REL_C, SRCE_SYST_C, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D, ROW_SECU_ACCS_C FROM %%DDSTG%%.ACCT_PATY_DEDUP WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS AP JOIN (SELECT T1.LOAN_I AS ACCT_I, UIP.ACCT_I AS ASSC_ACCT_I, (CASE WHEN UIP.EFFT_D > T1.EFFT_D THEN UIP.EFFT_D ELSE T1.EFFT_D END) AS EFFT_D, (CASE WHEN UIP.EXPY_D < T1.EXPY_D THEN UIP.EXPY_D ELSE T1.EXPY_D END) AS EXPY_D FROM (SELECT LOAN.LOAN_I, FCLY.SRCE_SYST_PATY_I, (CASE WHEN LOAN.EFFT_D > FCLY.EFFT_D THEN LOAN.EFFT_D ELSE FCLY.EFFT_D END) AS EFFT_D, (CASE WHEN LOAN.EXPY_D < FCLY.EXPY_D THEN LOAN.EXPY_D ELSE FCLY.EXPY_D END) AS EXPY_D FROM (SELECT LOAN_I, FCLY_I, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D FROM %%VTECH%%.MOS_LOAN WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS LOAN JOIN (SELECT SUBSTRING(FCLY_I, 1, 14) AS MOS_FCLY_I, SRCE_SYST_PATY_I, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D FROM %%VTECH%%.MOS_FCLY WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS FCLY ON FCLY.MOS_FCLY_I = LOAN.FCLY_I WHERE ((FCLY.EFFT_D BETWEEN LOAN.EFFT_D AND LOAN.EXPY_D) OR (LOAN.EFFT_D BETWEEN FCLY.EFFT_D AND FCLY.EXPY_D))) AS T1 JOIN (SELECT ACCT_I, SRCE_SYST_PATY_I, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D FROM %%VTECH%%.ACCT_UNID_PATY WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D AND SRCE_SYST_C = 'SAP' AND PATY_ACCT_REL_C = 'ACTO') AS UIP ON UIP.SRCE_SYST_PATY_I = T1.SRCE_SYST_PATY_I WHERE ((UIP.EFFT_D BETWEEN T1.EFFT_D AND T1.EXPY_D) OR (T1.EFFT_D BETWEEN UIP.EFFT_D AND UIP.EXPY_D))) AS AX ON AX.ASSC_ACCT_I = AP.ACCT_I WHERE ((AX.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D) OR (AP.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D)) GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9 UNION ALL SELECT AX.ACCT_I, AP.PATY_I, AX.ASSC_ACCT_I, AP.PATY_ACCT_REL_C, 'N' AS PRFR_PATY_F, AP.SRCE_SYST_C, (CASE WHEN AP.EFFT_D > AX.EFFT_D THEN AP.EFFT_D ELSE AX.EFFT_D END) AS EFFT_D, (CASE WHEN AP.EXPY_D < AX.EXPY_D THEN AP.EXPY_D ELSE AX.EXPY_D END) AS EXPY_D, AP.ROW_SECU_ACCS_C FROM (SELECT ACCT_I, PATY_I, PATY_ACCT_REL_C, SRCE_SYST_C, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D, ROW_SECU_ACCS_C FROM %%DDSTG%%.ACCT_PATY_DEDUP WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS AP JOIN (SELECT FCLY.FCLY_I AS ACCT_I, UIP.ACCT_I AS ASSC_ACCT_I, (CASE WHEN UIP.EFFT_D > FCLY.EFFT_D THEN UIP.EFFT_D ELSE FCLY.EFFT_D END) AS EFFT_D, (CASE WHEN UIP.EXPY_D < FCLY.EXPY_D THEN UIP.EXPY_D ELSE FCLY.EXPY_D END) AS EXPY_D FROM (SELECT FCLY_I, SRCE_SYST_PATY_I, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D FROM %%VTECH%%.MOS_FCLY WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D) AS FCLY JOIN (SELECT ACCT_I, SRCE_SYST_PATY_I, EFFT_D, CASE WHEN EFFT_D = EXPY_D THEN EXPY_D WHEN EXPY_D >= :EXTR_D THEN CAST('9999-12-31' AS DATE) ELSE EXPY_D END AS EXPY_D FROM %%VTECH%%.ACCT_UNID_PATY WHERE :EXTR_D BETWEEN EFFT_D AND EXPY_D AND SRCE_SYST_C = 'SAP' AND PATY_ACCT_REL_C = 'ACTO') AS UIP ON UIP.SRCE_SYST_PATY_I = FCLY.SRCE_SYST_PATY_I WHERE ((UIP.EFFT_D BETWEEN FCLY.EFFT_D AND FCLY.EXPY_D) OR (FCLY.EFFT_D BETWEEN UIP.EFFT_D AND UIP.EXPY_D))) AS AX ON AX.ASSC_ACCT_I = AP.ACCT_I WHERE ((AX.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D) OR (AP.EFFT_D BETWEEN AX.EFFT_D AND AX.EXPY_D)) GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."derv_acct_paty_curr"
WITH "ap" AS (
  SELECT
    "acct_paty_dedup"."acct_i" AS "acct_i",
    "acct_paty_dedup"."paty_i" AS "paty_i",
    "acct_paty_dedup"."paty_acct_rel_c" AS "paty_acct_rel_c",
    "acct_paty_dedup"."srce_syst_c" AS "srce_syst_c",
    "acct_paty_dedup"."efft_d" AS "efft_d",
    CASE
      WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
      THEN "acct_paty_dedup"."expy_d"
      WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "acct_paty_dedup"."expy_d"
    END AS "expy_d",
    "acct_paty_dedup"."row_secu_accs_c" AS "row_secu_accs_c"
  FROM "%%ddstg%%"."acct_paty_dedup" AS "acct_paty_dedup"
  WHERE
    "acct_paty_dedup"."efft_d" <= :EXTR_D
    AND CASE
      WHEN "acct_paty_dedup"."efft_d" = "acct_paty_dedup"."expy_d"
      THEN "acct_paty_dedup"."expy_d"
      WHEN "acct_paty_dedup"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "acct_paty_dedup"."expy_d"
    END >= :EXTR_D
), "uip" AS (
  SELECT
    "acct_unid_paty"."acct_i" AS "acct_i",
    "acct_unid_paty"."srce_syst_paty_i" AS "srce_syst_paty_i",
    "acct_unid_paty"."efft_d" AS "efft_d",
    CASE
      WHEN "acct_unid_paty"."efft_d" = "acct_unid_paty"."expy_d"
      THEN "acct_unid_paty"."expy_d"
      WHEN "acct_unid_paty"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "acct_unid_paty"."expy_d"
    END AS "expy_d"
  FROM "%%vtech%%"."acct_unid_paty" AS "acct_unid_paty"
  WHERE
    "acct_unid_paty"."efft_d" <= :EXTR_D
    AND "acct_unid_paty"."paty_acct_rel_c" = 'ACTO'
    AND "acct_unid_paty"."srce_syst_c" = 'SAP'
    AND CASE
      WHEN "acct_unid_paty"."efft_d" = "acct_unid_paty"."expy_d"
      THEN "acct_unid_paty"."expy_d"
      WHEN "acct_unid_paty"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "acct_unid_paty"."expy_d"
    END >= :EXTR_D
), "ax" AS (
  SELECT
    "mos_loan"."loan_i" AS "acct_i",
    "uip"."acct_i" AS "assc_acct_i",
    CASE
      WHEN "uip"."efft_d" > CASE
        WHEN "mos_fcly"."efft_d" < "mos_loan"."efft_d"
        THEN "mos_loan"."efft_d"
        ELSE "mos_fcly"."efft_d"
      END
      THEN "uip"."efft_d"
      ELSE CASE
        WHEN "mos_fcly"."efft_d" < "mos_loan"."efft_d"
        THEN "mos_loan"."efft_d"
        ELSE "mos_fcly"."efft_d"
      END
    END AS "efft_d",
    CASE
      WHEN "uip"."expy_d" < CASE
        WHEN CASE
          WHEN "mos_fcly"."efft_d" = "mos_fcly"."expy_d"
          THEN "mos_fcly"."expy_d"
          WHEN "mos_fcly"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "mos_fcly"."expy_d"
        END > CASE
          WHEN "mos_loan"."efft_d" = "mos_loan"."expy_d"
          THEN "mos_loan"."expy_d"
          WHEN "mos_loan"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "mos_loan"."expy_d"
        END
        THEN CASE
          WHEN "mos_loan"."efft_d" = "mos_loan"."expy_d"
          THEN "mos_loan"."expy_d"
          WHEN "mos_loan"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "mos_loan"."expy_d"
        END
        ELSE CASE
          WHEN "mos_fcly"."efft_d" = "mos_fcly"."expy_d"
          THEN "mos_fcly"."expy_d"
          WHEN "mos_fcly"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "mos_fcly"."expy_d"
        END
      END
      THEN "uip"."expy_d"
      ELSE CASE
        WHEN CASE
          WHEN "mos_fcly"."efft_d" = "mos_fcly"."expy_d"
          THEN "mos_fcly"."expy_d"
          WHEN "mos_fcly"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "mos_fcly"."expy_d"
        END > CASE
          WHEN "mos_loan"."efft_d" = "mos_loan"."expy_d"
          THEN "mos_loan"."expy_d"
          WHEN "mos_loan"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "mos_loan"."expy_d"
        END
        THEN CASE
          WHEN "mos_loan"."efft_d" = "mos_loan"."expy_d"
          THEN "mos_loan"."expy_d"
          WHEN "mos_loan"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "mos_loan"."expy_d"
        END
        ELSE CASE
          WHEN "mos_fcly"."efft_d" = "mos_fcly"."expy_d"
          THEN "mos_fcly"."expy_d"
          WHEN "mos_fcly"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "mos_fcly"."expy_d"
        END
      END
    END AS "expy_d"
  FROM "%%vtech%%"."mos_loan" AS "mos_loan"
  JOIN "%%vtech%%"."mos_fcly" AS "mos_fcly"
    ON "mos_fcly"."efft_d" <= :EXTR_D
    AND "mos_loan"."fcly_i" = SUBSTRING("mos_fcly"."fcly_i", 1, 14)
    AND (
      (
        "mos_fcly"."efft_d" <= "mos_loan"."efft_d"
        AND "mos_loan"."efft_d" <= CASE
          WHEN "mos_fcly"."efft_d" = "mos_fcly"."expy_d"
          THEN "mos_fcly"."expy_d"
          WHEN "mos_fcly"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "mos_fcly"."expy_d"
        END
      )
      OR (
        "mos_fcly"."efft_d" <= CASE
          WHEN "mos_loan"."efft_d" = "mos_loan"."expy_d"
          THEN "mos_loan"."expy_d"
          WHEN "mos_loan"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "mos_loan"."expy_d"
        END
        AND "mos_fcly"."efft_d" >= "mos_loan"."efft_d"
      )
    )
    AND CASE
      WHEN "mos_fcly"."efft_d" = "mos_fcly"."expy_d"
      THEN "mos_fcly"."expy_d"
      WHEN "mos_fcly"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "mos_fcly"."expy_d"
    END >= :EXTR_D
  JOIN "uip" AS "uip"
    ON "mos_fcly"."srce_syst_paty_i" = "uip"."srce_syst_paty_i"
    AND (
      (
        "uip"."efft_d" <= CASE
          WHEN "mos_fcly"."efft_d" < "mos_loan"."efft_d"
          THEN "mos_loan"."efft_d"
          ELSE "mos_fcly"."efft_d"
        END
        AND "uip"."expy_d" >= CASE
          WHEN "mos_fcly"."efft_d" < "mos_loan"."efft_d"
          THEN "mos_loan"."efft_d"
          ELSE "mos_fcly"."efft_d"
        END
      )
      OR (
        "uip"."efft_d" <= CASE
          WHEN CASE
            WHEN "mos_fcly"."efft_d" = "mos_fcly"."expy_d"
            THEN "mos_fcly"."expy_d"
            WHEN "mos_fcly"."expy_d" >= :EXTR_D
            THEN CAST('9999-12-31' AS DATE)
            ELSE "mos_fcly"."expy_d"
          END > CASE
            WHEN "mos_loan"."efft_d" = "mos_loan"."expy_d"
            THEN "mos_loan"."expy_d"
            WHEN "mos_loan"."expy_d" >= :EXTR_D
            THEN CAST('9999-12-31' AS DATE)
            ELSE "mos_loan"."expy_d"
          END
          THEN CASE
            WHEN "mos_loan"."efft_d" = "mos_loan"."expy_d"
            THEN "mos_loan"."expy_d"
            WHEN "mos_loan"."expy_d" >= :EXTR_D
            THEN CAST('9999-12-31' AS DATE)
            ELSE "mos_loan"."expy_d"
          END
          ELSE CASE
            WHEN "mos_fcly"."efft_d" = "mos_fcly"."expy_d"
            THEN "mos_fcly"."expy_d"
            WHEN "mos_fcly"."expy_d" >= :EXTR_D
            THEN CAST('9999-12-31' AS DATE)
            ELSE "mos_fcly"."expy_d"
          END
        END
        AND "uip"."efft_d" >= CASE
          WHEN "mos_fcly"."efft_d" < "mos_loan"."efft_d"
          THEN "mos_loan"."efft_d"
          ELSE "mos_fcly"."efft_d"
        END
      )
    )
  WHERE
    "mos_loan"."efft_d" <= :EXTR_D
    AND (
      (
        "mos_fcly"."efft_d" <= "mos_loan"."efft_d"
        AND "mos_loan"."efft_d" <= CASE
          WHEN "mos_fcly"."efft_d" = "mos_fcly"."expy_d"
          THEN "mos_fcly"."expy_d"
          WHEN "mos_fcly"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "mos_fcly"."expy_d"
        END
      )
      OR (
        "mos_fcly"."efft_d" <= CASE
          WHEN "mos_loan"."efft_d" = "mos_loan"."expy_d"
          THEN "mos_loan"."expy_d"
          WHEN "mos_loan"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "mos_loan"."expy_d"
        END
        AND "mos_fcly"."efft_d" >= "mos_loan"."efft_d"
      )
    )
    AND (
      (
        "uip"."efft_d" <= CASE
          WHEN "mos_fcly"."efft_d" < "mos_loan"."efft_d"
          THEN "mos_loan"."efft_d"
          ELSE "mos_fcly"."efft_d"
        END
        AND "uip"."expy_d" >= CASE
          WHEN "mos_fcly"."efft_d" < "mos_loan"."efft_d"
          THEN "mos_loan"."efft_d"
          ELSE "mos_fcly"."efft_d"
        END
      )
      OR (
        "uip"."efft_d" <= CASE
          WHEN CASE
            WHEN "mos_fcly"."efft_d" = "mos_fcly"."expy_d"
            THEN "mos_fcly"."expy_d"
            WHEN "mos_fcly"."expy_d" >= :EXTR_D
            THEN CAST('9999-12-31' AS DATE)
            ELSE "mos_fcly"."expy_d"
          END > CASE
            WHEN "mos_loan"."efft_d" = "mos_loan"."expy_d"
            THEN "mos_loan"."expy_d"
            WHEN "mos_loan"."expy_d" >= :EXTR_D
            THEN CAST('9999-12-31' AS DATE)
            ELSE "mos_loan"."expy_d"
          END
          THEN CASE
            WHEN "mos_loan"."efft_d" = "mos_loan"."expy_d"
            THEN "mos_loan"."expy_d"
            WHEN "mos_loan"."expy_d" >= :EXTR_D
            THEN CAST('9999-12-31' AS DATE)
            ELSE "mos_loan"."expy_d"
          END
          ELSE CASE
            WHEN "mos_fcly"."efft_d" = "mos_fcly"."expy_d"
            THEN "mos_fcly"."expy_d"
            WHEN "mos_fcly"."expy_d" >= :EXTR_D
            THEN CAST('9999-12-31' AS DATE)
            ELSE "mos_fcly"."expy_d"
          END
        END
        AND "uip"."efft_d" >= CASE
          WHEN "mos_fcly"."efft_d" < "mos_loan"."efft_d"
          THEN "mos_loan"."efft_d"
          ELSE "mos_fcly"."efft_d"
        END
      )
    )
    AND CASE
      WHEN "mos_loan"."efft_d" = "mos_loan"."expy_d"
      THEN "mos_loan"."expy_d"
      WHEN "mos_loan"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "mos_loan"."expy_d"
    END >= :EXTR_D
), "ax_2" AS (
  SELECT
    "mos_fcly"."fcly_i" AS "acct_i",
    "uip"."acct_i" AS "assc_acct_i",
    CASE
      WHEN "mos_fcly"."efft_d" < "uip"."efft_d"
      THEN "uip"."efft_d"
      ELSE "mos_fcly"."efft_d"
    END AS "efft_d",
    CASE
      WHEN "uip"."expy_d" < CASE
        WHEN "mos_fcly"."efft_d" = "mos_fcly"."expy_d"
        THEN "mos_fcly"."expy_d"
        WHEN "mos_fcly"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "mos_fcly"."expy_d"
      END
      THEN "uip"."expy_d"
      ELSE CASE
        WHEN "mos_fcly"."efft_d" = "mos_fcly"."expy_d"
        THEN "mos_fcly"."expy_d"
        WHEN "mos_fcly"."expy_d" >= :EXTR_D
        THEN CAST('9999-12-31' AS DATE)
        ELSE "mos_fcly"."expy_d"
      END
    END AS "expy_d"
  FROM "%%vtech%%"."mos_fcly" AS "mos_fcly"
  JOIN "uip" AS "uip"
    ON "mos_fcly"."srce_syst_paty_i" = "uip"."srce_syst_paty_i"
    AND (
      (
        "mos_fcly"."efft_d" <= "uip"."efft_d"
        AND "uip"."efft_d" <= CASE
          WHEN "mos_fcly"."efft_d" = "mos_fcly"."expy_d"
          THEN "mos_fcly"."expy_d"
          WHEN "mos_fcly"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "mos_fcly"."expy_d"
        END
      )
      OR (
        "mos_fcly"."efft_d" <= "uip"."expy_d" AND "mos_fcly"."efft_d" >= "uip"."efft_d"
      )
    )
  WHERE
    "mos_fcly"."efft_d" <= :EXTR_D
    AND (
      (
        "mos_fcly"."efft_d" <= "uip"."efft_d"
        AND "uip"."efft_d" <= CASE
          WHEN "mos_fcly"."efft_d" = "mos_fcly"."expy_d"
          THEN "mos_fcly"."expy_d"
          WHEN "mos_fcly"."expy_d" >= :EXTR_D
          THEN CAST('9999-12-31' AS DATE)
          ELSE "mos_fcly"."expy_d"
        END
      )
      OR (
        "mos_fcly"."efft_d" <= "uip"."expy_d" AND "mos_fcly"."efft_d" >= "uip"."efft_d"
      )
    )
    AND CASE
      WHEN "mos_fcly"."efft_d" = "mos_fcly"."expy_d"
      THEN "mos_fcly"."expy_d"
      WHEN "mos_fcly"."expy_d" >= :EXTR_D
      THEN CAST('9999-12-31' AS DATE)
      ELSE "mos_fcly"."expy_d"
    END >= :EXTR_D
)
SELECT
  "ax"."acct_i" AS "acct_i",
  "ap"."paty_i" AS "paty_i",
  "ax"."assc_acct_i" AS "assc_acct_i",
  "ap"."paty_acct_rel_c" AS "paty_acct_rel_c",
  'N' AS "prfr_paty_f",
  "ap"."srce_syst_c" AS "srce_syst_c",
  (
    CASE WHEN "ap"."efft_d" > "ax"."efft_d" THEN "ap"."efft_d" ELSE "ax"."efft_d" END
  ) AS "efft_d",
  (
    CASE WHEN "ap"."expy_d" < "ax"."expy_d" THEN "ap"."expy_d" ELSE "ax"."expy_d" END
  ) AS "expy_d",
  "ap"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "ap" AS "ap"
JOIN "ax" AS "ax"
  ON "ap"."acct_i" = "ax"."assc_acct_i"
  AND (
    (
      "ap"."efft_d" <= "ax"."efft_d" AND "ap"."expy_d" >= "ax"."efft_d"
    )
    OR (
      "ap"."efft_d" <= "ax"."expy_d" AND "ap"."efft_d" >= "ax"."efft_d"
    )
  )
WHERE
  (
    "ap"."efft_d" <= "ax"."efft_d" AND "ap"."expy_d" >= "ax"."efft_d"
  )
  OR (
    "ap"."efft_d" <= "ax"."expy_d" AND "ap"."efft_d" >= "ax"."efft_d"
  )
GROUP BY
  "ax"."acct_i",
  "ap"."paty_i",
  "ax"."assc_acct_i",
  "ap"."paty_acct_rel_c",
  5,
  "ap"."srce_syst_c",
  (
    CASE WHEN "ap"."efft_d" > "ax"."efft_d" THEN "ap"."efft_d" ELSE "ax"."efft_d" END
  ),
  (
    CASE WHEN "ap"."expy_d" < "ax"."expy_d" THEN "ap"."expy_d" ELSE "ax"."expy_d" END
  ),
  "ap"."row_secu_accs_c"
UNION ALL
SELECT
  "ax"."acct_i" AS "acct_i",
  "ap"."paty_i" AS "paty_i",
  "ax"."assc_acct_i" AS "assc_acct_i",
  "ap"."paty_acct_rel_c" AS "paty_acct_rel_c",
  'N' AS "prfr_paty_f",
  "ap"."srce_syst_c" AS "srce_syst_c",
  (
    CASE WHEN "ap"."efft_d" > "ax"."efft_d" THEN "ap"."efft_d" ELSE "ax"."efft_d" END
  ) AS "efft_d",
  (
    CASE WHEN "ap"."expy_d" < "ax"."expy_d" THEN "ap"."expy_d" ELSE "ax"."expy_d" END
  ) AS "expy_d",
  "ap"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "ap" AS "ap"
JOIN "ax_2" AS "ax"
  ON "ap"."acct_i" = "ax"."assc_acct_i"
  AND (
    (
      "ap"."efft_d" <= "ax"."efft_d" AND "ap"."expy_d" >= "ax"."efft_d"
    )
    OR (
      "ap"."efft_d" <= "ax"."expy_d" AND "ap"."efft_d" >= "ax"."efft_d"
    )
  )
WHERE
  (
    "ap"."efft_d" <= "ax"."efft_d" AND "ap"."expy_d" >= "ax"."efft_d"
  )
  OR (
    "ap"."efft_d" <= "ax"."expy_d" AND "ap"."efft_d" >= "ax"."efft_d"
  )
GROUP BY
  "ax"."acct_i",
  "ap"."paty_i",
  "ax"."assc_acct_i",
  "ap"."paty_acct_rel_c",
  5,
  "ap"."srce_syst_c",
  (
    CASE WHEN "ap"."efft_d" > "ax"."efft_d" THEN "ap"."efft_d" ELSE "ax"."efft_d" END
  ),
  (
    CASE WHEN "ap"."expy_d" < "ax"."expy_d" THEN "ap"."expy_d" ELSE "ax"."expy_d" END
  ),
  "ap"."row_secu_accs_c"
```

#### 📊 SQL Metadata:
- **Tables**: ACCT_PATY_DEDUP, ACCT_UNID_PATY, DERV_ACCT_PATY_CURR, MOS_FCLY, MOS_LOAN
- **Columns**: ACCT_I, ASSC_ACCT_I, EFFT_D, EXPY_D, FCLY_I, LOAN_I, MOS_FCLY_I, PATY_ACCT_REL_C, PATY_I, ROW_SECU_ACCS_C, SRCE_SYST_C, SRCE_SYST_PATY_I
- **Functions**: '9999-12-31', (AX.EFFT_D BETWEEN AP.EFFT_D AND AP.EXPY_D), (FCLY.EFFT_D BETWEEN LOAN.EFFT_D AND LOAN.EXPY_D), (UIP.EFFT_D BETWEEN FCLY.EFFT_D AND FCLY.EXPY_D), (UIP.EFFT_D BETWEEN T1.EFFT_D AND T1.EXPY_D), :EXTR_D BETWEEN EFFT_D AND EXPY_D, :EXTR_D BETWEEN EFFT_D AND EXPY_D AND SRCE_SYST_C = 'SAP', AP.EFFT_D > AX.EFFT_D, AP.EXPY_D < AX.EXPY_D, EFFT_D = EXPY_D, EXPY_D >= :EXTR_D, FCLY_I, LOAN.EFFT_D > FCLY.EFFT_D, LOAN.EXPY_D < FCLY.EXPY_D, None, UIP.EFFT_D > FCLY.EFFT_D, UIP.EFFT_D > T1.EFFT_D, UIP.EXPY_D < FCLY.EXPY_D, UIP.EXPY_D < T1.EXPY_D

## Migration Recommendations

### Suggested Migration Strategy
**High complexity** - Break into multiple models, use DCF full framework

### DCF Mapping
- **Error Handling**: Convert `.IF ERRORCODE` patterns to DCF error handling macros
- **File Operations**: Replace IMPORT/EXPORT with dbt seeds or external tables
- **Statistics**: Replace COLLECT STATS with Snowflake post-hooks
- **Stored Procedures**: Map CALL statements to DCF stored procedure macros

---

*Generated by BTEQ Parser Service - 2025-08-20 12:39:22*
