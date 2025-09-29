# DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG_CHG0379808.sql - BTEQ Analysis

## File Overview
- **File Name**: DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG_CHG0379808.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 13
- **SQL Blocks**: 5

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 38 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 63 | IF_ERRORCODE | `   .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 65 | COLLECT_STATS | `   COLLECT STATS    %%DDSTG%%.DERV_ACCT_PATY_NON_RM;` |
| 66 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 79 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 127 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 153 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 162 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 196 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 198 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_FLAG;` |
| 200 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 207 | LABEL | `.LABEL EXITERR` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 36-37)
- **Complexity Score**: 6
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
DELETE FROM %%DDSTG%%.DERV_ACCT_PATY_NON_RM ALL;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%DDSTG%%.DERV_ACCT_PATY_NON_RM AS ALL
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%ddstg%%"."derv_acct_paty_non_rm" AS "all"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_ACCT_PATY_NON_RM

### SQL Block 2 (Lines 41-62)
- **Complexity Score**: 80
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO  %%DDSTG%%.DERV_ACCT_PATY_NON_RM

SELECT 
       DAP.ACCT_I
      ,DAP.PATY_I
      ,DAP.ASSC_ACCT_I
      ,DAP.PATY_ACCT_REL_C
     ,DAP. PRFR_PATY_F
     ,DAP.SRCE_SYST_C
      ,DAP.EFFT_D
      ,DAP.EXPY_D
      ,DAP.ROW_SECU_ACCS_C
	  ,99 AS RANK_I /* C2039849_IM1317265_Revenue_Attribution_to_Fund_Holder for Non RM Account - Adding RANK_I is a part of Fund Holder Project */

FROM %%DDSTG%%.DERV_ACCT_PATY_CURR DAP
LEFT JOIN %%DDSTG%%.DERV_PRTF_ACCT_PATY_STAG T1

  ON DAP.ASSC_ACCT_I = T1.ACCT_I
WHERE :EXTR_D BETWEEN DAP.EFFT_D AND DAP.EXPY_D
AND T1.ACCT_I IS NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_NON_RM SELECT DAP.ACCT_I, DAP.PATY_I, DAP.ASSC_ACCT_I, DAP.PATY_ACCT_REL_C, DAP.PRFR_PATY_F, DAP.SRCE_SYST_C, DAP.EFFT_D, DAP.EXPY_D, DAP.ROW_SECU_ACCS_C, 99 AS RANK_I FROM %%DDSTG%%.DERV_ACCT_PATY_CURR AS DAP LEFT JOIN %%DDSTG%%.DERV_PRTF_ACCT_PATY_STAG AS T1 ON DAP.ASSC_ACCT_I = T1.ACCT_I WHERE :EXTR_D BETWEEN DAP.EFFT_D AND DAP.EXPY_D AND T1.ACCT_I IS NULL GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."derv_acct_paty_non_rm"
SELECT
  "dap"."acct_i" AS "acct_i",
  "dap"."paty_i" AS "paty_i",
  "dap"."assc_acct_i" AS "assc_acct_i",
  "dap"."paty_acct_rel_c" AS "paty_acct_rel_c",
  "dap"."prfr_paty_f" AS "prfr_paty_f",
  "dap"."srce_syst_c" AS "srce_syst_c",
  "dap"."efft_d" AS "efft_d",
  "dap"."expy_d" AS "expy_d",
  "dap"."row_secu_accs_c" AS "row_secu_accs_c",
  99 AS "rank_i"
FROM "%%ddstg%%"."derv_acct_paty_curr" AS "dap"
LEFT JOIN "%%ddstg%%"."derv_prtf_acct_paty_stag" AS "t1"
  ON "dap"."assc_acct_i" = "t1"."acct_i"
WHERE
  "dap"."efft_d" <= :EXTR_D AND "dap"."expy_d" >= :EXTR_D AND "t1"."acct_i" IS NULL
GROUP BY
  "dap"."acct_i",
  "dap"."paty_i",
  "dap"."assc_acct_i",
  "dap"."paty_acct_rel_c",
  "dap"."prfr_paty_f",
  "dap"."srce_syst_c",
  "dap"."efft_d",
  "dap"."expy_d",
  "dap"."row_secu_accs_c",
  10
```

#### 📊 SQL Metadata:
- **Tables**: DERV_ACCT_PATY_CURR, DERV_ACCT_PATY_NON_RM, DERV_PRTF_ACCT_PATY_STAG
- **Columns**: ACCT_I, ASSC_ACCT_I, EFFT_D, EXPY_D, PATY_ACCT_REL_C, PATY_I, PRFR_PATY_F, ROW_SECU_ACCS_C, SRCE_SYST_C
- **Functions**: :EXTR_D BETWEEN DAP.EFFT_D AND DAP.EXPY_D

### SQL Block 3 (Lines 82-126)
- **Complexity Score**: 182
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 3

#### 📝 Original Teradata SQL:
```sql
UPDATE  DAPNR
  FROM  %%DDSTG%%.DERV_ACCT_PATY_NON_RM  DAPNR,
  	(SELECT DISTINCT  PIG.PATY_I			  			  
    	 	          ,GDFVC.PRTY
          FROM  PVDATA.PATY_INT_GRUP_CURR PIG 				
    INNER JOIN  PVDATA.INT_GRUP_DEPT_CURR IGD 				
      	     ON  IGD.INT_GRUP_I=PIG.INT_GRUP_I			 						   
           AND  PIG.REL_C = 'RLMT'
    INNER JOIN (   	SELECT  GFC.DEPT_LEAF_NODE_C AS DEPT_I
       			,ORU.PRTY PRTY 
		 	 FROM   %%VTECH%%.GRD_DEPT_FLAT_CURR  GFC 
		   LEFT JOIN  (
				   SELECT  COALESCE(PRTY,9999) PRTY 
	      				   ,LKUP1_TEXT
		      			   ,COALESCE(UPDT_DTTS,CRAT_DTTS) LoadTimeStamp
   		  		     FROM  %%VTECH%%.ODS_RULE --%%VTECH%% Should be replaced by 
 				    WHERE  RULE_CODE = 'RMPOC'			
		  		      AND :EXTR_D BETWEEN VALD_FROM AND VALD_TO
	       		  QUALIFY  ROW_NUMBER() OVER (PARTITION BY LKUP1_TEXT,PRTY ORDER BY LoadTimeStamp DESC) = 1		
		     		 )ORU
		          ON   GFC.DEPT_L3_NODE_C = ORU.LKUP1_TEXT
		       WHERE   PRTY <> 9999 ) GDFVC
	     ON   GDFVC.DEPT_I = IGD.DEPT_I
 	  WHERE   PIG.PATY_I IN (  SELECT  DISTINCT PATY_I 
	      			    	 FROM  %%DDSTG%%.DERV_ACCT_PATY_CURR)		     			    	 	 		 	
	QUALIFY   ROW_NUMBER() OVER (PARTITION BY PIG.PATY_I ORDER BY GDFVC.PRTY ASC) = 1)DRVD
   SET  RANK_I = DRVD.PRTY
 WHERE  DAPNR.PATY_I = DRVD.PATY_I
   AND  DAPNR.PATY_ACCT_REL_C= 'ZINTE0';

UPDATE  %%DDSTG%%.DERV_ACCT_PATY_NON_RM
   SET  RANK_I = CASE WHEN RANK_I <> 99 AND PATY_ACCT_REL_C = 'ZINTE0' 	   /* Any Fund Holder relationship with With Account would be a part of Rule Priority Wil get First Priroty */   	           	 
   		        THEN RANK_I 		      
	 	        WHEN PATY_ACCT_REL_C = 'ZINTE0'          		   /* Any Fund Holder Part of FundHolder Would Get Second Priority */
	 	        THEN 98 	      
			 ELSE 99            				      		   /* Any Remaining Would Get Least Prirotty */
      		    END;

/* END   C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder Change for NON RM Accounts   */			 


 
-- Determine MAX(PATY_I) for each account and, as all rows are current, remove
-- those that have EFFT_D less than the effective date of the row with MAX(PATY_I)
```

#### ❄️ Converted Snowflake SQL:
```sql
UPDATE DAPNR SET RANK_I = DRVD.PRTY FROM %%DDSTG%%.DERV_ACCT_PATY_NON_RM AS DAPNR, (SELECT DISTINCT PIG.PATY_I, GDFVC.PRTY FROM PVDATA.PATY_INT_GRUP_CURR AS PIG INNER JOIN PVDATA.INT_GRUP_DEPT_CURR AS IGD ON IGD.INT_GRUP_I = PIG.INT_GRUP_I AND PIG.REL_C = 'RLMT' INNER JOIN (SELECT GFC.DEPT_LEAF_NODE_C AS DEPT_I, ORU.PRTY AS PRTY FROM %%VTECH%%.GRD_DEPT_FLAT_CURR AS GFC LEFT JOIN (SELECT COALESCE(PRTY, 9999) AS PRTY, LKUP1_TEXT, COALESCE(UPDT_DTTS, CRAT_DTTS) AS LoadTimeStamp FROM %%VTECH%%.ODS_RULE WHERE RULE_CODE = 'RMPOC' AND :EXTR_D BETWEEN VALD_FROM AND VALD_TO QUALIFY ROW_NUMBER() OVER (PARTITION BY LKUP1_TEXT, PRTY ORDER BY LoadTimeStamp DESC NULLS LAST) = 1) AS ORU ON GFC.DEPT_L3_NODE_C = ORU.LKUP1_TEXT WHERE PRTY <> 9999) AS GDFVC ON GDFVC.DEPT_I = IGD.DEPT_I WHERE PIG.PATY_I IN (SELECT DISTINCT PATY_I FROM %%DDSTG%%.DERV_ACCT_PATY_CURR) QUALIFY ROW_NUMBER() OVER (PARTITION BY PIG.PATY_I ORDER BY GDFVC.PRTY ASC NULLS FIRST) = 1) AS DRVD WHERE DAPNR.PATY_I = DRVD.PATY_I AND DAPNR.PATY_ACCT_REL_C = 'ZINTE0'
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- QUALIFY clause
- ROW_NUMBER() OVER
- Variable substitution

#### ⚡ Optimized SQL:
```sql
WITH "_u_0" AS (
  SELECT DISTINCT
    "derv_acct_paty_curr"."paty_i" AS "paty_i"
  FROM "%%ddstg%%"."derv_acct_paty_curr" AS "derv_acct_paty_curr"
  GROUP BY
    "derv_acct_paty_curr"."paty_i"
), "oru" AS (
  SELECT
    COALESCE("ods_rule"."prty", 9999) AS "prty",
    "ods_rule"."lkup1_text" AS "lkup1_text"
  FROM "%%vtech%%"."ods_rule" AS "ods_rule"
  WHERE
    "ods_rule"."rule_code" = 'RMPOC'
    AND "ods_rule"."vald_from" <= :EXTR_D
    AND "ods_rule"."vald_to" >= :EXTR_D
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY "ods_rule"."lkup1_text", COALESCE("prty", 9999)
      ORDER BY COALESCE("ods_rule"."updt_dtts", "ods_rule"."crat_dtts") DESC NULLS LAST
    ) = 1
)
UPDATE "dapnr" SET "drvd"."prty" = "rank_i"
FROM "%%ddstg%%"."derv_acct_paty_non_rm" AS "dapnr"
  CROSS JOIN (
    SELECT DISTINCT
      "pig"."paty_i" AS "paty_i",
      "oru"."prty" AS "prty"
    FROM "pvdata"."paty_int_grup_curr" AS "pig"
    LEFT JOIN "_u_0" AS "_u_0"
      ON "_u_0"."paty_i" = "pig"."paty_i"
    JOIN "pvdata"."int_grup_dept_curr" AS "igd"
      ON "igd"."int_grup_i" = "pig"."int_grup_i" AND "pig"."rel_c" = 'RLMT'
    JOIN "%%vtech%%"."grd_dept_flat_curr" AS "gfc"
      ON "gfc"."dept_leaf_node_c" = "igd"."dept_i"
    LEFT JOIN "oru" AS "oru"
      ON "gfc"."dept_l3_node_c" = "oru"."lkup1_text"
    WHERE
      "oru"."prty" <> 9999 AND NOT "_u_0"."paty_i" IS NULL
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY "pig"."paty_i" ORDER BY "oru"."prty" NULLS FIRST) = 1
  ) AS "drvd"
WHERE
  "dapnr"."paty_acct_rel_c" = 'ZINTE0' AND "dapnr"."paty_i" = "drvd"."paty_i"
```

#### 📊 SQL Metadata:
- **Tables**: DAPNR, DERV_ACCT_PATY_CURR, DERV_ACCT_PATY_NON_RM, GRD_DEPT_FLAT_CURR, INT_GRUP_DEPT_CURR, ODS_RULE, PATY_INT_GRUP_CURR
- **Columns**: CRAT_DTTS, DEPT_I, DEPT_L3_NODE_C, DEPT_LEAF_NODE_C, INT_GRUP_I, LKUP1_TEXT, LoadTimeStamp, PATY_ACCT_REL_C, PATY_I, PRTY, RANK_I, REL_C, RULE_CODE, UPDT_DTTS, VALD_FROM, VALD_TO
- **Functions**: DAPNR.PATY_I = DRVD.PATY_I, IGD.INT_GRUP_I = PIG.INT_GRUP_I, None, PRTY, RULE_CODE = 'RMPOC', UPDT_DTTS
- **Window_Functions**: None

### SQL Block 4 (Lines 130-152)
- **Complexity Score**: 0
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ❌ Invalid
- **Teradata Features**: 3

#### 📝 Original Teradata SQL:
```sql
UPDATE DAP
  FROM %%DDSTG%%.DERV_ACCT_PATY_NON_RM DAP,
      (SELECT T1.ACCT_I      
	      ,T1.PATY_I
	      ,T1.PATY_aCCT_REL_C
	      ,T1.EFFT_D
	      ,T1.EXPY_D
        FROM  %%DDSTG%%.DERV_ACCT_PATY_NON_RM T1
        JOIN 	%%DDSTG%%.GRD_GNRC_MAP_DERV_PATY_HOLD T2  
          ON  T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C /* Exclude blank and numeric accounts  */ 
       WHERE  UPPER(TRIM(T1.ACCT_I))(CASESPECIFIC) <> LOWER(TRIM(T1.ACCT_I))(CASESPECIFIC)
         AND 	T1.EFFT_D <= T1.EXPY_D     
	  AND  :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D /*QUALIFY ROW_NUMBER() OVER (PARTITION BY T1.ACCT_I ORDER BY T1.PATY_I DESC, T1.PATY_ACCT_REL_C) = 1) T3*/
     QUALIFY ROW_NUMBER() OVER (PARTITION BY T1.ACCT_I ORDER BY T1.RANK_I ASC,T1.PATY_I DESC, T1.PATY_ACCT_REL_C) = 1 
	   /* C2039849_IM1317265_ Revenue_Attribution_to_Fund_Holder - Added Rank_I To the Query */
     )T3
  SET PRFR_PATY_F = 'Y'
WHERE DAP.ACCT_I = T3.ACCT_I
  AND DAP.PATY_I = T3.PATY_I
  AND DAP.PATY_ACCT_REL_C = T3.PATY_ACCT_REL_C
  AND DAP.EFFT_D = T3.EFFT_D
  AND DAP.EXPY_D = T3.EXPY_D;
```

#### ❄️ Converted Snowflake SQL:
```sql
UPDATE DAP
  FROM %%DDSTG%%.DERV_ACCT_PATY_NON_RM DAP,
      (SELECT T1.ACCT_I      
	      ,T1.PATY_I
	      ,T1.PATY_aCCT_REL_C
	      ,T1.EFFT_D
	      ,T1.EXPY_D
        FROM  %%DDSTG%%.DERV_ACCT_PATY_NON_RM T1
        JOIN 	%%DDSTG%%.GRD_GNRC_MAP_DERV_PATY_HOLD T2  
          ON  T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C /* Exclude blank and numeric accounts  */ 
       WHERE  UPPER(TRIM(T1.ACCT_I))(CASESPECIFIC) <> LOWER(TRIM(T1.ACCT_I))(CASESPECIFIC)
         AND 	T1.EFFT_D <= T1.EXPY_D     
	  AND  :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_D /*QUALIFY ROW_NUMBER() OVER (PARTITION BY T1.ACCT_I ORDER BY T1.PATY_I DESC, T1.PATY_ACCT_REL_C) = 1) T3*/
     QUALIFY ROW_NUMBER() OVER (PARTITION BY T1.ACCT_I ORDER BY T1.RANK_I ASC,T1.PATY_I DESC, T1.PATY_ACCT_REL_C) = 1 
	   /* C2039849_IM1317265_ Revenue_Attribution_to_Fund_Holder - Added Rank_I To the Query */
     )T3
  SET PRFR_PATY_F = 'Y'
WHERE DAP.ACCT_I = T3.ACCT_I
  AND DAP.PATY_I = T3.PATY_I
  AND DAP.PATY_ACCT_REL_C = T3.PATY_ACCT_REL_C
  AND DAP.EFFT_D = T3.EFFT_D
  AND DAP.EXPY_D = T3.EXPY_D;
```

#### 🔍 Syntax Validation Details:
- **Valid**: ❌

#### 🎯 Teradata Features Detected:
- QUALIFY clause
- ROW_NUMBER() OVER
- Variable substitution

### SQL Block 5 (Lines 166-195)
- **Complexity Score**: 37
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_FLAG
SELECT 
 ACCT_I                        
,PATY_I                        
,ASSC_ACCT_I                   
,PATY_ACCT_REL_C               
,PRFR_PATY_F                   
,SRCE_SYST_C                   
,EFFT_D                        
,EXPY_D 
,ROW_SECU_ACCS_C                       

FROM %%DDSTG%%.DERV_ACCT_PATY_RM
GROUP BY 1,2,3,4,5,6,7,8,9
;INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_FLAG
SELECT 
 ACCT_I                        
,PATY_I                        
,ASSC_ACCT_I                   
,PATY_ACCT_REL_C               
,PRFR_PATY_F                   
,SRCE_SYST_C                   
,EFFT_D                        
,EXPY_D 
,ROW_SECU_ACCS_C                       

FROM %%DDSTG%%.DERV_ACCT_PATY_NON_RM
GROUP BY 1,2,3,4,5,6,7,8,9
;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_FLAG SELECT ACCT_I, PATY_I, ASSC_ACCT_I, PATY_ACCT_REL_C, PRFR_PATY_F, SRCE_SYST_C, EFFT_D, EXPY_D, ROW_SECU_ACCS_C FROM %%DDSTG%%.DERV_ACCT_PATY_RM GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."derv_acct_paty_flag"
SELECT
  "derv_acct_paty_rm"."acct_i" AS "acct_i",
  "derv_acct_paty_rm"."paty_i" AS "paty_i",
  "derv_acct_paty_rm"."assc_acct_i" AS "assc_acct_i",
  "derv_acct_paty_rm"."paty_acct_rel_c" AS "paty_acct_rel_c",
  "derv_acct_paty_rm"."prfr_paty_f" AS "prfr_paty_f",
  "derv_acct_paty_rm"."srce_syst_c" AS "srce_syst_c",
  "derv_acct_paty_rm"."efft_d" AS "efft_d",
  "derv_acct_paty_rm"."expy_d" AS "expy_d",
  "derv_acct_paty_rm"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "%%ddstg%%"."derv_acct_paty_rm" AS "derv_acct_paty_rm"
GROUP BY
  "derv_acct_paty_rm"."acct_i",
  "derv_acct_paty_rm"."paty_i",
  "derv_acct_paty_rm"."assc_acct_i",
  "derv_acct_paty_rm"."paty_acct_rel_c",
  "derv_acct_paty_rm"."prfr_paty_f",
  "derv_acct_paty_rm"."srce_syst_c",
  "derv_acct_paty_rm"."efft_d",
  "derv_acct_paty_rm"."expy_d",
  "derv_acct_paty_rm"."row_secu_accs_c"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_ACCT_PATY_FLAG, DERV_ACCT_PATY_RM
- **Columns**: ACCT_I, ASSC_ACCT_I, EFFT_D, EXPY_D, PATY_ACCT_REL_C, PATY_I, PRFR_PATY_F, ROW_SECU_ACCS_C, SRCE_SYST_C

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
