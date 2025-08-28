# ACCT_BALN_BKDT_RECN_GET_PROS_KEY.sql - BTEQ Analysis

## File Overview
- **File Name**: ACCT_BALN_BKDT_RECN_GET_PROS_KEY.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 6
- **SQL Blocks**: 1

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 2 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 164 | IF_ERRORCODE | `.IF ERRORCODE <> 0 THEN .GOTO EXITERR` |
| 168 | LOGOFF | `.LOGOFF` |
| 171 | LABEL | `.LABEL EXITERR` |
| 173 | LOGOFF | `.LOGOFF` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 26-163)
- **Complexity Score**: 297
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%CAD_PROD_DATA%%.UTIL_PROS_ISAC
(
PROS_KEY_I                    
,CONV_M                        
,CONV_TYPE_M                   
,PROS_RQST_S                   
,PROS_LAST_RQST_S              
,PROS_RQST_Q                   
,BTCH_RUN_D                    
,BTCH_KEY_I                    
,SRCE_SYST_M                   
,SRCE_M                        
,TRGT_M                        
,SUCC_F                        
,COMT_F                        
,COMT_S                        
,MLTI_LOAD_EFFT_D              
,SYST_S                        
,MLTI_LOAD_COMT_S              
,SYST_ET_Q                     
,SYST_UV_Q                     
,SYST_INS_Q                    
,SYST_UPD_Q                    
,SYST_DEL_Q                    
,SYST_ET_TABL_M                
,SYST_UV_TABL_M                
,SYST_HEAD_ET_TABL_M           
,SYST_HEAD_UV_TABL_M           
,SYST_TRLR_ET_TABL_M           
,SYST_TRLR_UV_TABL_M           
,PREV_PROS_KEY_I               
,HEAD_RECD_TYPE_C              
,HEAD_FILE_M                   
,HEAD_BTCH_RUN_D               
,HEAD_FILE_CRAT_S              
,HEAD_GENR_PRGM_M              
,HEAD_BTCH_KEY_I               
,HEAD_PROS_KEY_I               
,HEAD_PROS_PREV_KEY_I          
,TRLR_RECD_TYPE_C              
,TRLR_RECD_Q                   
,TRLR_HASH_TOTL_A              
,TRLR_COLM_HASH_TOTL_M         
,TRLR_EROR_RECD_Q              
,TRLR_FILE_COMT_S              
,TRLR_RECD_ISRT_Q              
,TRLR_RECD_UPDT_Q              
,TRLR_RECD_DELT_Q              
)
SELECT 
PARM_LTRL_N+1	AS PROS_KEY_I, 
'CAD_X01_ACCT_BALN_BKDT_RECN'  AS  CONV_M , 
'TD' AS  CONV_TYPE_M,
CURRENT_TIMESTAMP(0) AS  PROS_RQST_S, 
CURRENT_TIMESTAMP(0) AS  PROS_LAST_RQST_S,
1 AS  PROS_RQST_Q,
/*Inserts multiple records for each batch run in the event of any delays or failures*/
DT.CALR_CALR_D AS  BTCH_RUN_D,
/*As this solution is not part of any batch [eg, SAP], the Batch Key is populated as null*/
NULL AS  BTCH_KEY_I ,
/*Sourcing from the GDW itself and so the source system name is GDW*/
'GDW' AS  SRCE_SYST_M,
/*Applying the adjustments from ACCT_BALN on ACCT_BALN_BKDT_RECN*/
'ACCT_BALN_BKDT' AS  SRCE_M ,
'ACCT_BALN_BKDT_RECN' AS  TRGT_M ,
'N' AS  SUCC_F ,
'N' AS  COMT_F ,
NULL AS  COMT_S ,
NULL AS  MLTI_LOAD_EFFT_D,
NULL AS  SYST_S ,
NULL AS  MLTI_LOAD_COMT_S,
NULL AS  SYST_ET_Q  ,
NULL AS  SYST_UV_Q  ,
NULL AS  SYST_INS_Q ,
NULL AS  SYST_UPD_Q ,
NULL AS  SYST_DEL_Q ,
NULL AS  SYST_ET_TABL_M  ,
NULL AS  SYST_UV_TABL_M  ,
NULL AS  SYST_HEAD_ET_TABL_M ,
NULL AS  SYST_HEAD_UV_TABL_M ,
NULL AS  SYST_TRLR_ET_TABL_M ,
NULL AS  SYST_TRLR_UV_TABL_M ,
NULL AS  PREV_PROS_KEY_I ,
NULL AS  HEAD_RECD_TYPE_C,
NULL AS  HEAD_FILE_M,
NULL AS  HEAD_BTCH_RUN_D ,
NULL AS  HEAD_FILE_CRAT_S,
NULL AS  HEAD_GENR_PRGM_M,
NULL AS  HEAD_BTCH_KEY_I ,
NULL AS  HEAD_PROS_KEY_I ,
NULL AS  HEAD_PROS_PREV_KEY_I,
NULL AS  TRLR_RECD_TYPE_C,
NULL AS  TRLR_RECD_Q,
NULL AS  TRLR_HASH_TOTL_A,
NULL AS  TRLR_COLM_HASH_TOTL_M,
NULL AS  TRLR_EROR_RECD_Q,
NULL AS  TRLR_FILE_COMT_S,
NULL AS  TRLR_RECD_ISRT_Q,
NULL AS  TRLR_RECD_UPDT_Q,
NULL AS  TRLR_RECD_DELT_Q
FROM 
%%VTECH%%.UTIL_PARM PARM
CROSS JOIN 
/*Inserts multiple records for each batch run in the event of any delays or failures. 
Capture latest Batch run date relating to the Latest delta load into ACCT_BALN_BKDT*/
(SELECT  
CAL.CALR_CALR_D
FROM
/*Capture last Succesful Batch run date relating to the Backdated adjustment solution */
(SELECT  MAX(BTCH_RUN_D) AS BTCH_RUN_D 
FROM  %%VTECH%%.UTIL_PROS_ISAC
WHERE    
TRGT_M='ACCT_BALN_BKDT' 
AND SRCE_SYST_M='GDW'
AND COMT_F = 'Y'  
AND SUCC_F='Y') BKDT_PREV
CROSS JOIN 
/*Capture latest Batch run date relating to the Backdated adjustment solution into ACCT_BALN_BKDT*/
(SELECT  MAX(BTCH_RUN_D) AS BTCH_RUN_D 
FROM  %%VTECH%%.UTIL_PROS_ISAC
WHERE    
TRGT_M='ACCT_BALN_BKDT' 
AND SRCE_SYST_M='GDW') BKDT_CURR

,%%VTECH%%.GRD_RPRT_CALR_CLYR CAL

WHERE CAL.CALR_CALR_D > BKDT_PREV.BTCH_RUN_D 
AND CAL.CALR_CALR_D  <= BKDT_CURR.BTCH_RUN_D
)DT
WHERE 
PARM.PARM_M='PROS_KEY'
 
 /*Increment PROS KEY by 1 and update UTIL PARM table*/
;UPDATE %%CAD_PROD_DATA%%.UTIL_PARM
SET PARM_LTRL_N = PARM_LTRL_N + 1
WHERE
PARM_M='PROS_KEY';
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%CAD_PROD_DATA%%.UTIL_PROS_ISAC (PROS_KEY_I, CONV_M, CONV_TYPE_M, PROS_RQST_S, PROS_LAST_RQST_S, PROS_RQST_Q, BTCH_RUN_D, BTCH_KEY_I, SRCE_SYST_M, SRCE_M, TRGT_M, SUCC_F, COMT_F, COMT_S, MLTI_LOAD_EFFT_D, SYST_S, MLTI_LOAD_COMT_S, SYST_ET_Q, SYST_UV_Q, SYST_INS_Q, SYST_UPD_Q, SYST_DEL_Q, SYST_ET_TABL_M, SYST_UV_TABL_M, SYST_HEAD_ET_TABL_M, SYST_HEAD_UV_TABL_M, SYST_TRLR_ET_TABL_M, SYST_TRLR_UV_TABL_M, PREV_PROS_KEY_I, HEAD_RECD_TYPE_C, HEAD_FILE_M, HEAD_BTCH_RUN_D, HEAD_FILE_CRAT_S, HEAD_GENR_PRGM_M, HEAD_BTCH_KEY_I, HEAD_PROS_KEY_I, HEAD_PROS_PREV_KEY_I, TRLR_RECD_TYPE_C, TRLR_RECD_Q, TRLR_HASH_TOTL_A, TRLR_COLM_HASH_TOTL_M, TRLR_EROR_RECD_Q, TRLR_FILE_COMT_S, TRLR_RECD_ISRT_Q, TRLR_RECD_UPDT_Q, TRLR_RECD_DELT_Q) SELECT PARM_LTRL_N + 1 AS PROS_KEY_I, 'CAD_X01_ACCT_BALN_BKDT_RECN' AS CONV_M, 'TD' AS CONV_TYPE_M, CURRENT_TIMESTAMP(0) AS PROS_RQST_S, CURRENT_TIMESTAMP(0) AS PROS_LAST_RQST_S, 1 AS PROS_RQST_Q, DT.CALR_CALR_D AS BTCH_RUN_D, NULL AS BTCH_KEY_I, 'GDW' AS SRCE_SYST_M, 'ACCT_BALN_BKDT' AS SRCE_M, 'ACCT_BALN_BKDT_RECN' AS TRGT_M, 'N' AS SUCC_F, 'N' AS COMT_F, NULL AS COMT_S, NULL AS MLTI_LOAD_EFFT_D, NULL AS SYST_S, NULL AS MLTI_LOAD_COMT_S, NULL AS SYST_ET_Q, NULL AS SYST_UV_Q, NULL AS SYST_INS_Q, NULL AS SYST_UPD_Q, NULL AS SYST_DEL_Q, NULL AS SYST_ET_TABL_M, NULL AS SYST_UV_TABL_M, NULL AS SYST_HEAD_ET_TABL_M, NULL AS SYST_HEAD_UV_TABL_M, NULL AS SYST_TRLR_ET_TABL_M, NULL AS SYST_TRLR_UV_TABL_M, NULL AS PREV_PROS_KEY_I, NULL AS HEAD_RECD_TYPE_C, NULL AS HEAD_FILE_M, NULL AS HEAD_BTCH_RUN_D, NULL AS HEAD_FILE_CRAT_S, NULL AS HEAD_GENR_PRGM_M, NULL AS HEAD_BTCH_KEY_I, NULL AS HEAD_PROS_KEY_I, NULL AS HEAD_PROS_PREV_KEY_I, NULL AS TRLR_RECD_TYPE_C, NULL AS TRLR_RECD_Q, NULL AS TRLR_HASH_TOTL_A, NULL AS TRLR_COLM_HASH_TOTL_M, NULL AS TRLR_EROR_RECD_Q, NULL AS TRLR_FILE_COMT_S, NULL AS TRLR_RECD_ISRT_Q, NULL AS TRLR_RECD_UPDT_Q, NULL AS TRLR_RECD_DELT_Q FROM %%VTECH%%.UTIL_PARM AS PARM CROSS JOIN (SELECT CAL.CALR_CALR_D FROM (SELECT MAX(BTCH_RUN_D) AS BTCH_RUN_D FROM %%VTECH%%.UTIL_PROS_ISAC WHERE TRGT_M = 'ACCT_BALN_BKDT' AND SRCE_SYST_M = 'GDW' AND COMT_F = 'Y' AND SUCC_F = 'Y') AS BKDT_PREV CROSS JOIN (SELECT MAX(BTCH_RUN_D) AS BTCH_RUN_D FROM %%VTECH%%.UTIL_PROS_ISAC WHERE TRGT_M = 'ACCT_BALN_BKDT' AND SRCE_SYST_M = 'GDW') AS BKDT_CURR, %%VTECH%%.GRD_RPRT_CALR_CLYR AS CAL WHERE CAL.CALR_CALR_D > BKDT_PREV.BTCH_RUN_D AND CAL.CALR_CALR_D <= BKDT_CURR.BTCH_RUN_D) AS DT WHERE PARM.PARM_M = 'PROS_KEY'
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%cad_prod_data%%"."util_pros_isac" (
  "pros_key_i",
  "conv_m",
  "conv_type_m",
  "pros_rqst_s",
  "pros_last_rqst_s",
  "pros_rqst_q",
  "btch_run_d",
  "btch_key_i",
  "srce_syst_m",
  "srce_m",
  "trgt_m",
  "succ_f",
  "comt_f",
  "comt_s",
  "mlti_load_efft_d",
  "syst_s",
  "mlti_load_comt_s",
  "syst_et_q",
  "syst_uv_q",
  "syst_ins_q",
  "syst_upd_q",
  "syst_del_q",
  "syst_et_tabl_m",
  "syst_uv_tabl_m",
  "syst_head_et_tabl_m",
  "syst_head_uv_tabl_m",
  "syst_trlr_et_tabl_m",
  "syst_trlr_uv_tabl_m",
  "prev_pros_key_i",
  "head_recd_type_c",
  "head_file_m",
  "head_btch_run_d",
  "head_file_crat_s",
  "head_genr_prgm_m",
  "head_btch_key_i",
  "head_pros_key_i",
  "head_pros_prev_key_i",
  "trlr_recd_type_c",
  "trlr_recd_q",
  "trlr_hash_totl_a",
  "trlr_colm_hash_totl_m",
  "trlr_eror_recd_q",
  "trlr_file_comt_s",
  "trlr_recd_isrt_q",
  "trlr_recd_updt_q",
  "trlr_recd_delt_q"
)
WITH "bkdt_prev" AS (
  SELECT
    MAX("util_pros_isac"."btch_run_d") AS "btch_run_d"
  FROM "%%vtech%%"."util_pros_isac" AS "util_pros_isac"
  WHERE
    "util_pros_isac"."comt_f" = 'Y'
    AND "util_pros_isac"."srce_syst_m" = 'GDW'
    AND "util_pros_isac"."succ_f" = 'Y'
    AND "util_pros_isac"."trgt_m" = 'ACCT_BALN_BKDT'
), "bkdt_curr" AS (
  SELECT
    MAX("util_pros_isac"."btch_run_d") AS "btch_run_d"
  FROM "%%vtech%%"."util_pros_isac" AS "util_pros_isac"
  WHERE
    "util_pros_isac"."srce_syst_m" = 'GDW'
    AND "util_pros_isac"."trgt_m" = 'ACCT_BALN_BKDT'
)
SELECT
  "parm"."parm_ltrl_n" + 1 AS "pros_key_i",
  'CAD_X01_ACCT_BALN_BKDT_RECN' AS "conv_m",
  'TD' AS "conv_type_m",
  CURRENT_TIMESTAMP(0) AS "pros_rqst_s",
  CURRENT_TIMESTAMP(0) AS "pros_last_rqst_s",
  1 AS "pros_rqst_q",
  "cal"."calr_calr_d" AS "btch_run_d",
  NULL AS "btch_key_i",
  'GDW' AS "srce_syst_m",
  'ACCT_BALN_BKDT' AS "srce_m",
  'ACCT_BALN_BKDT_RECN' AS "trgt_m",
  'N' AS "succ_f",
  'N' AS "comt_f",
  NULL AS "comt_s",
  NULL AS "mlti_load_efft_d",
  NULL AS "syst_s",
  NULL AS "mlti_load_comt_s",
  NULL AS "syst_et_q",
  NULL AS "syst_uv_q",
  NULL AS "syst_ins_q",
  NULL AS "syst_upd_q",
  NULL AS "syst_del_q",
  NULL AS "syst_et_tabl_m",
  NULL AS "syst_uv_tabl_m",
  NULL AS "syst_head_et_tabl_m",
  NULL AS "syst_head_uv_tabl_m",
  NULL AS "syst_trlr_et_tabl_m",
  NULL AS "syst_trlr_uv_tabl_m",
  NULL AS "prev_pros_key_i",
  NULL AS "head_recd_type_c",
  NULL AS "head_file_m",
  NULL AS "head_btch_run_d",
  NULL AS "head_file_crat_s",
  NULL AS "head_genr_prgm_m",
  NULL AS "head_btch_key_i",
  NULL AS "head_pros_key_i",
  NULL AS "head_pros_prev_key_i",
  NULL AS "trlr_recd_type_c",
  NULL AS "trlr_recd_q",
  NULL AS "trlr_hash_totl_a",
  NULL AS "trlr_colm_hash_totl_m",
  NULL AS "trlr_eror_recd_q",
  NULL AS "trlr_file_comt_s",
  NULL AS "trlr_recd_isrt_q",
  NULL AS "trlr_recd_updt_q",
  NULL AS "trlr_recd_delt_q"
FROM "%%vtech%%"."util_parm" AS "parm"
CROSS JOIN "bkdt_prev" AS "bkdt_prev"
JOIN "%%vtech%%"."grd_rprt_calr_clyr" AS "cal"
  ON "bkdt_prev"."btch_run_d" < "cal"."calr_calr_d"
JOIN "bkdt_curr" AS "bkdt_curr"
  ON "bkdt_curr"."btch_run_d" >= "cal"."calr_calr_d"
WHERE
  "parm"."parm_m" = 'PROS_KEY'
```

#### 📊 SQL Metadata:
- **Tables**: GRD_RPRT_CALR_CLYR, UTIL_PARM, UTIL_PROS_ISAC
- **Columns**: BTCH_RUN_D, CALR_CALR_D, COMT_F, PARM_LTRL_N, PARM_M, SRCE_SYST_M, SUCC_F, TRGT_M
- **Functions**: 0, BTCH_RUN_D, CAL.CALR_CALR_D > BKDT_PREV.BTCH_RUN_D, TRGT_M = 'ACCT_BALN_BKDT', TRGT_M = 'ACCT_BALN_BKDT' AND SRCE_SYST_M = 'GDW', TRGT_M = 'ACCT_BALN_BKDT' AND SRCE_SYST_M = 'GDW' AND COMT_F = 'Y'

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
