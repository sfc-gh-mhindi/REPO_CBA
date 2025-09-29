# DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG.sql - BTEQ Analysis

## File Overview
- **File Name**: DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 19
- **SQL Blocks**: 7

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 39 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 64 | IF_ERRORCODE | `   .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 66 | COLLECT_STATS | `   COLLECT STATS    %%DDSTG%%.DERV_ACCT_PATY_NON_RM;` |
| 67 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 80 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 128 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 154 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 163 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 197 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 199 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_FLAG;` |
| 201 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 213 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 228 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 231 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 320 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 322 | COLLECT_STATS | `COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_FLAG;` |
| 324 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 332 | LABEL | `.LABEL EXITERR` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 37-38)
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

### SQL Block 2 (Lines 42-63)
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

### SQL Block 3 (Lines 83-127)
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

### SQL Block 4 (Lines 131-153)
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
     QUALIFY ROW_NUMBER() OVER (PARTITION BY T1.ACCT_I ORDER BY T1.RANK_I ASC,T1.PATY_I ASC, T1.PATY_ACCT_REL_C) = 1 
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
     QUALIFY ROW_NUMBER() OVER (PARTITION BY T1.ACCT_I ORDER BY T1.RANK_I ASC,T1.PATY_I ASC, T1.PATY_ACCT_REL_C) = 1 
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

### SQL Block 5 (Lines 167-196)
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

### SQL Block 6 (Lines 216-227)
- **Complexity Score**: 53
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
UPDATE %%DDSTG%%.DERV_ACCT_PATY_FLAG
SET PRFR_PATY_F = 'N'
WHERE ACCT_I IN
(SELECT DISTINCT DAP.ACCT_I
FROM %%DDSTG%%.DERV_ACCT_PATY_FLAG DAP
INNER JOIN %%VTECH%%.GRD_GNRC_MAP_CURR DPH
ON DPH.MAP_TYPE_C	= 'DERV_ACCT_PATY_HOLD_REL_MAP'
AND DPH.SRCE_CHAR_1_C = DAP.PATY_ACCT_REL_C
GROUP BY 1 HAVING Count(DISTINCT DAP.PATY_I) > 1
)
;
```

#### ❄️ Converted Snowflake SQL:
```sql
UPDATE %%DDSTG%%.DERV_ACCT_PATY_FLAG SET PRFR_PATY_F = 'N' WHERE ACCT_I IN (SELECT DISTINCT DAP.ACCT_I FROM %%DDSTG%%.DERV_ACCT_PATY_FLAG AS DAP INNER JOIN %%VTECH%%.GRD_GNRC_MAP_CURR AS DPH ON DPH.MAP_TYPE_C = 'DERV_ACCT_PATY_HOLD_REL_MAP' AND DPH.SRCE_CHAR_1_C = DAP.PATY_ACCT_REL_C GROUP BY 1 HAVING COUNT(DISTINCT DAP.PATY_I) > 1)
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
UPDATE "%%ddstg%%"."derv_acct_paty_flag" SET "prfr_paty_f" = 'N'
WHERE
  "acct_i" IN (
    SELECT DISTINCT
      "dap"."acct_i" AS "acct_i"
    FROM "%%ddstg%%"."derv_acct_paty_flag" AS "dap"
    JOIN "%%vtech%%"."grd_gnrc_map_curr" AS "dph"
      ON "dap"."paty_acct_rel_c" = "dph"."srce_char_1_c"
      AND "dph"."map_type_c" = 'DERV_ACCT_PATY_HOLD_REL_MAP'
    GROUP BY
      "dap"."acct_i"
    HAVING
      COUNT(DISTINCT "dap"."paty_i") > 1
  )
```

#### 📊 SQL Metadata:
- **Tables**: DERV_ACCT_PATY_FLAG, GRD_GNRC_MAP_CURR
- **Columns**: ACCT_I, MAP_TYPE_C, PATY_ACCT_REL_C, PATY_I, PRFR_PATY_F, SRCE_CHAR_1_C
- **Functions**: DISTINCT DAP.PATY_I, DPH.MAP_TYPE_C = 'DERV_ACCT_PATY_HOLD_REL_MAP'

### SQL Block 7 (Lines 234-319)
- **Complexity Score**: 367
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 3

#### 📝 Original Teradata SQL:
```sql
UPDATE DAP1
FROM %%DDSTG%%.DERV_ACCT_PATY_FLAG DAP1,
(
 SELECT
  DAP.ACCT_I
 ,DAP.PATY_I
 ,DAP.PATY_ACCT_REL_C
 ,DAP.EXPY_D
 FROM %%DDSTG%%.DERV_ACCT_PATY_FLAG DAP

 --Below table contains the list of Roles which needs to be considered along with the Priority.
 --Priority is required because an Account can have multiple parties and each Party can belong to multiple roles.
 --Eventually for a given account, we need just 1 Party from only 1 Relationship code to be flagged as Preferred Party Flag = 'Y'
 INNER JOIN %%DDSTG%%.GRD_GNRC_MAP_PATY_HOLD_PRTY DPH
 ON  DPH.MAP_TYPE_C	= 'DERV_ACCT_PATY_HOLD_REL_MAP'
 AND DPH.PATY_ACCT_REL_C = DAP.PATY_ACCT_REL_C
 
 --To get the Portfolio to which the Party belong
 LEFT JOIN %%VTECH%%.PATY_INT_GRUP PI1
 ON  PI1.PATY_I	= DAP.PATY_I
 AND PI1.REL_C	= 'RLMT'
 AND :EXTR_D BETWEEN PI1.EFFT_D AND PI1.EXPY_D
 AND :EXTR_D BETWEEN PI1.VALD_FROM_D AND PI1.VALD_TO_D
 
 --To get the Parent Department to which the Portfolio belong
 LEFT JOIN %%VTECH%%.INT_GRUP_DEPT IGD
 ON  IGD.INT_GRUP_I	= PI1.INT_GRUP_I
 AND :EXTR_D BETWEEN IGD.EFFT_D AND IGD.EXPY_D
 AND :EXTR_D BETWEEN IGD.VALD_FROM_D AND IGD.VALD_TO_D

 --To get the Business Segments for which priority has been defined
 LEFT JOIN
 (
  SELECT
   DA1.DEPT_I
  ,BSP1.BUSN_SEGM_C    AS DEPT_NODE_C
  ,BSP1.BUSN_SEGM_X	   AS DEPT_NODE_M
  ,BSP1.BUSN_SEGM_PRTY AS BUSN_SEGM_PRTY_N
  FROM %%VTECH%%.DEPT_DIMN_NODE_ANCS_CURR DA1
  INNER JOIN %%DDSTG%%.GRD_GNRC_MAP_BUSN_SEGM_PRTY BSP1 --Priority of different Departments defined here
  ON DA1.ANCS_DEPT_I = BSP1.BUSN_SEGM_C
  AND BSP1.MAP_TYPE_C = 'BUSN_SEGM_PRTY'
 ) SEGM_PRTY
 ON SEGM_PRTY.DEPT_I = IGD.DEPT_I

 --To check if the Portfolio is Banker Managed or Department managed
 LEFT JOIN %%VTECH%%.DERV_PRTF_OWN_REL DPOR
 ON  DPOR.PRTF_CODE_X = Right(PI1.INT_GRUP_I,7)
 AND DPOR.ROLE_PLAY_TYPE_X = 'Employee'
 AND DPOR.DERV_PRTF_TYPE_C = 'RLMT'
 AND DPOR.PRTF_CODE_X <> 'NA'
 AND :EXTR_D BETWEEN DPOR.VALD_FROM_D AND DPOR.VALD_TO_D
 AND :EXTR_D BETWEEN DPOR.EFFT_D AND DPOR.EXPY_D

 --Updating only those accounts which have more than 1 unique Party Id
 WHERE DAP.ACCT_I IN (SELECT ACCT_I FROM %%DDSTG%%.DERV_ACCT_PATY_FLAG GROUP BY 1 HAVING Count(DISTINCT PATY_I) > 1)

 --1st priority is for Party with ZINTE0 - Fund Holder Relationship code
 --2nd priority is based on Business Segments to which the Portfolio of the Parties belong
 --3rd prirotiy is for Banker or Department managed portfolio. Higher priority for Banker Managed Portfolio
 --4th priority is limited to SBB portfolio. If the third last letter is 'B' which means Holding Portfolio, then give it less priority compared to Active Portfolio
 --5th priority is given to the earliest created Party Id
 --6th priority is just to ensure that for a given account, Preferred Party Flag is set for only 1 row, i.e. only for 1 party with 1 relationship code
 --Priority from 1 to 5 will give only 1 party but that party can belong to multiple roles, 6th priority will ensure that only 1 row gets updated with PRFR_PATY_F = 'Y'
 --EXPY_D is put on top of ORDER BY because there could be rows from source which are created and ended on the same day..
 --..So we need to take the latest row in such a case
 QUALIFY Row_Number() Over (PARTITION BY DAP.ACCT_I
							ORDER BY
								 DAP.EXPY_D DESC
								,CASE WHEN DPH.PATY_ACCT_REL_C = 'ZINTE0' THEN -1 ELSE 0 END ASC
								,Coalesce(SEGM_PRTY.BUSN_SEGM_PRTY_N,99) ASC
								,CASE WHEN DPOR.DERV_PRTF_ROLE_C = 'OWNR' THEN 1 WHEN DPOR.DERV_PRTF_ROLE_C = 'AOWN' THEN 2 WHEN DPOR.DERV_PRTF_ROLE_C = 'ASTT' THEN 3 ELSE 4 END ASC
								,CASE WHEN SEGM_PRTY.DEPT_NODE_C = 'NDEPT902744' /*Small Business Banking*/ AND Substring(Reverse(PI1.INT_GRUP_I), 3, 1) = 'B' THEN 1 ELSE 0 END ASC
								,DAP.PATY_I ASC
								,Coalesce(DPH.PATY_ACCT_REL_PRTY,99) ASC
							) = 1
) DT1

SET PRFR_PATY_F = 'Y'

WHERE DAP1.ACCT_I = DT1.ACCT_I
AND   DAP1.PATY_I = DT1.PATY_I
AND   DAP1.PATY_ACCT_REL_C = DT1.PATY_ACCT_REL_C
AND   DAP1.EXPY_D = DT1.EXPY_D
;
```

#### ❄️ Converted Snowflake SQL:
```sql
UPDATE DAP1 SET PRFR_PATY_F = 'Y' FROM %%DDSTG%%.DERV_ACCT_PATY_FLAG AS DAP1, (SELECT DAP.ACCT_I, DAP.PATY_I, DAP.PATY_ACCT_REL_C, DAP.EXPY_D FROM %%DDSTG%%.DERV_ACCT_PATY_FLAG AS DAP INNER JOIN %%DDSTG%%.GRD_GNRC_MAP_PATY_HOLD_PRTY AS DPH ON DPH.MAP_TYPE_C = 'DERV_ACCT_PATY_HOLD_REL_MAP' AND DPH.PATY_ACCT_REL_C = DAP.PATY_ACCT_REL_C LEFT JOIN %%VTECH%%.PATY_INT_GRUP AS PI1 ON PI1.PATY_I = DAP.PATY_I AND PI1.REL_C = 'RLMT' AND :EXTR_D BETWEEN PI1.EFFT_D AND PI1.EXPY_D AND :EXTR_D BETWEEN PI1.VALD_FROM_D AND PI1.VALD_TO_D LEFT JOIN %%VTECH%%.INT_GRUP_DEPT AS IGD ON IGD.INT_GRUP_I = PI1.INT_GRUP_I AND :EXTR_D BETWEEN IGD.EFFT_D AND IGD.EXPY_D AND :EXTR_D BETWEEN IGD.VALD_FROM_D AND IGD.VALD_TO_D LEFT JOIN (SELECT DA1.DEPT_I, BSP1.BUSN_SEGM_C AS DEPT_NODE_C, BSP1.BUSN_SEGM_X AS DEPT_NODE_M, BSP1.BUSN_SEGM_PRTY AS BUSN_SEGM_PRTY_N FROM %%VTECH%%.DEPT_DIMN_NODE_ANCS_CURR AS DA1 INNER JOIN %%DDSTG%%.GRD_GNRC_MAP_BUSN_SEGM_PRTY AS BSP1 ON DA1.ANCS_DEPT_I = BSP1.BUSN_SEGM_C AND BSP1.MAP_TYPE_C = 'BUSN_SEGM_PRTY') AS SEGM_PRTY ON SEGM_PRTY.DEPT_I = IGD.DEPT_I LEFT JOIN %%VTECH%%.DERV_PRTF_OWN_REL AS DPOR ON DPOR.PRTF_CODE_X = RIGHT(PI1.INT_GRUP_I, 7) AND DPOR.ROLE_PLAY_TYPE_X = 'Employee' AND DPOR.DERV_PRTF_TYPE_C = 'RLMT' AND DPOR.PRTF_CODE_X <> 'NA' AND :EXTR_D BETWEEN DPOR.VALD_FROM_D AND DPOR.VALD_TO_D AND :EXTR_D BETWEEN DPOR.EFFT_D AND DPOR.EXPY_D WHERE DAP.ACCT_I IN (SELECT ACCT_I FROM %%DDSTG%%.DERV_ACCT_PATY_FLAG GROUP BY 1 HAVING COUNT(DISTINCT PATY_I) > 1) QUALIFY ROW_NUMBER() OVER (PARTITION BY DAP.ACCT_I ORDER BY DAP.EXPY_D DESC NULLS LAST, CASE WHEN DPH.PATY_ACCT_REL_C = 'ZINTE0' THEN -1 ELSE 0 END ASC NULLS FIRST, COALESCE(SEGM_PRTY.BUSN_SEGM_PRTY_N, 99) ASC NULLS FIRST, CASE WHEN DPOR.DERV_PRTF_ROLE_C = 'OWNR' THEN 1 WHEN DPOR.DERV_PRTF_ROLE_C = 'AOWN' THEN 2 WHEN DPOR.DERV_PRTF_ROLE_C = 'ASTT' THEN 3 ELSE 4 END ASC NULLS FIRST, CASE WHEN SEGM_PRTY.DEPT_NODE_C = 'NDEPT902744' AND SUBSTRING(REVERSE(PI1.INT_GRUP_I), 3, 1) = 'B' THEN 1 ELSE 0 END ASC NULLS FIRST, DAP.PATY_I ASC NULLS FIRST, COALESCE(DPH.PATY_ACCT_REL_PRTY, 99) ASC NULLS FIRST) = 1) AS DT1 WHERE DAP1.ACCT_I = DT1.ACCT_I AND DAP1.PATY_I = DT1.PATY_I AND DAP1.PATY_ACCT_REL_C = DT1.PATY_ACCT_REL_C AND DAP1.EXPY_D = DT1.EXPY_D
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
  SELECT
    "derv_acct_paty_flag"."acct_i" AS "acct_i"
  FROM "%%ddstg%%"."derv_acct_paty_flag" AS "derv_acct_paty_flag"
  GROUP BY
    "derv_acct_paty_flag"."acct_i"
  HAVING
    COUNT(DISTINCT "derv_acct_paty_flag"."paty_i") > 1
)
UPDATE "dap1" SET "prfr_paty_f" = 'Y'
FROM "%%ddstg%%"."derv_acct_paty_flag" AS "dap1"
  CROSS JOIN (
    SELECT
      "dap"."acct_i" AS "acct_i",
      "dap"."paty_i" AS "paty_i",
      "dap"."paty_acct_rel_c" AS "paty_acct_rel_c",
      "dap"."expy_d" AS "expy_d"
    FROM "%%ddstg%%"."derv_acct_paty_flag" AS "dap"
    LEFT JOIN "_u_0" AS "_u_0"
      ON "_u_0"."acct_i" = "dap"."acct_i"
    JOIN "%%ddstg%%"."grd_gnrc_map_paty_hold_prty" AS "dph"
      ON "dap"."paty_acct_rel_c" = "dph"."paty_acct_rel_c"
      AND "dph"."map_type_c" = 'DERV_ACCT_PATY_HOLD_REL_MAP'
    LEFT JOIN "%%vtech%%"."paty_int_grup" AS "pi1"
      ON "dap"."paty_i" = "pi1"."paty_i"
      AND "pi1"."efft_d" <= :EXTR_D
      AND "pi1"."expy_d" >= :EXTR_D
      AND "pi1"."rel_c" = 'RLMT'
      AND "pi1"."vald_from_d" <= :EXTR_D
      AND "pi1"."vald_to_d" >= :EXTR_D
    LEFT JOIN "%%vtech%%"."derv_prtf_own_rel" AS "dpor"
      ON "dpor"."derv_prtf_type_c" = 'RLMT'
      AND "dpor"."efft_d" <= :EXTR_D
      AND "dpor"."expy_d" >= :EXTR_D
      AND "dpor"."prtf_code_x" <> 'NA'
      AND "dpor"."prtf_code_x" = RIGHT("pi1"."int_grup_i", 7)
      AND "dpor"."role_play_type_x" = 'Employee'
      AND "dpor"."vald_from_d" <= :EXTR_D
      AND "dpor"."vald_to_d" >= :EXTR_D
    LEFT JOIN "%%vtech%%"."int_grup_dept" AS "igd"
      ON "igd"."efft_d" <= :EXTR_D
      AND "igd"."expy_d" >= :EXTR_D
      AND "igd"."int_grup_i" = "pi1"."int_grup_i"
      AND "igd"."vald_from_d" <= :EXTR_D
      AND "igd"."vald_to_d" >= :EXTR_D
    LEFT JOIN "%%vtech%%"."dept_dimn_node_ancs_curr" AS "da1"
      ON "da1"."dept_i" = "igd"."dept_i"
    JOIN "%%ddstg%%"."grd_gnrc_map_busn_segm_prty" AS "bsp1"
      ON "bsp1"."busn_segm_c" = "da1"."ancs_dept_i"
      AND "bsp1"."map_type_c" = 'BUSN_SEGM_PRTY'
    WHERE
      NOT "_u_0"."acct_i" IS NULL
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY "dap"."acct_i"
        ORDER BY "dap"."expy_d" DESC NULLS LAST, CASE WHEN "dph"."paty_acct_rel_c" = 'ZINTE0' THEN -1 ELSE 0 END NULLS FIRST, COALESCE("bsp1"."busn_segm_prty", 99) NULLS FIRST, CASE
          WHEN "dpor"."derv_prtf_role_c" = 'OWNR'
          THEN 1
          WHEN "dpor"."derv_prtf_role_c" = 'AOWN'
          THEN 2
          WHEN "dpor"."derv_prtf_role_c" = 'ASTT'
          THEN 3
          ELSE 4
        END NULLS FIRST, CASE
          WHEN "bsp1"."busn_segm_c" = 'NDEPT902744'
          AND SUBSTRING(REVERSE("pi1"."int_grup_i"), 3, 1) = 'B'
          THEN 1
          ELSE 0
        END NULLS FIRST, "dap"."paty_i" NULLS FIRST, COALESCE("dph"."paty_acct_rel_prty", 99) NULLS FIRST
      ) = 1
  ) AS "dt1"
WHERE
  "dap1"."acct_i" = "dt1"."acct_i"
  AND "dap1"."expy_d" = "dt1"."expy_d"
  AND "dap1"."paty_acct_rel_c" = "dt1"."paty_acct_rel_c"
  AND "dap1"."paty_i" = "dt1"."paty_i"
```

#### 📊 SQL Metadata:
- **Tables**: DAP1, DEPT_DIMN_NODE_ANCS_CURR, DERV_ACCT_PATY_FLAG, DERV_PRTF_OWN_REL, GRD_GNRC_MAP_BUSN_SEGM_PRTY, GRD_GNRC_MAP_PATY_HOLD_PRTY, INT_GRUP_DEPT, PATY_INT_GRUP
- **Columns**: ACCT_I, ANCS_DEPT_I, BUSN_SEGM_C, BUSN_SEGM_PRTY, BUSN_SEGM_PRTY_N, BUSN_SEGM_X, DEPT_I, DEPT_NODE_C, DERV_PRTF_ROLE_C, DERV_PRTF_TYPE_C, EFFT_D, EXPY_D, INT_GRUP_I, MAP_TYPE_C, PATY_ACCT_REL_C, PATY_ACCT_REL_PRTY, PATY_I, PRFR_PATY_F, PRTF_CODE_X, REL_C, ROLE_PLAY_TYPE_X, VALD_FROM_D, VALD_TO_D
- **Functions**: DA1.ANCS_DEPT_I = BSP1.BUSN_SEGM_C, DAP1.ACCT_I = DT1.ACCT_I, DAP1.ACCT_I = DT1.ACCT_I AND DAP1.PATY_I = DT1.PATY_I, DAP1.ACCT_I = DT1.ACCT_I AND DAP1.PATY_I = DT1.PATY_I AND DAP1.PATY_ACCT_REL_C = DT1.PATY_ACCT_REL_C, DISTINCT PATY_I, DPH.MAP_TYPE_C = 'DERV_ACCT_PATY_HOLD_REL_MAP', DPH.PATY_ACCT_REL_C = 'ZINTE0', DPH.PATY_ACCT_REL_PRTY, DPOR.DERV_PRTF_ROLE_C = 'AOWN', DPOR.DERV_PRTF_ROLE_C = 'ASTT', DPOR.DERV_PRTF_ROLE_C = 'OWNR', DPOR.PRTF_CODE_X = RIGHT(PI1.INT_GRUP_I, 7), DPOR.PRTF_CODE_X = RIGHT(PI1.INT_GRUP_I, 7) AND DPOR.ROLE_PLAY_TYPE_X = 'Employee', DPOR.PRTF_CODE_X = RIGHT(PI1.INT_GRUP_I, 7) AND DPOR.ROLE_PLAY_TYPE_X = 'Employee' AND DPOR.DERV_PRTF_TYPE_C = 'RLMT', DPOR.PRTF_CODE_X = RIGHT(PI1.INT_GRUP_I, 7) AND DPOR.ROLE_PLAY_TYPE_X = 'Employee' AND DPOR.DERV_PRTF_TYPE_C = 'RLMT' AND DPOR.PRTF_CODE_X <> 'NA', DPOR.PRTF_CODE_X = RIGHT(PI1.INT_GRUP_I, 7) AND DPOR.ROLE_PLAY_TYPE_X = 'Employee' AND DPOR.DERV_PRTF_TYPE_C = 'RLMT' AND DPOR.PRTF_CODE_X <> 'NA' AND :EXTR_D BETWEEN DPOR.VALD_FROM_D AND DPOR.VALD_TO_D, IGD.INT_GRUP_I = PI1.INT_GRUP_I, IGD.INT_GRUP_I = PI1.INT_GRUP_I AND :EXTR_D BETWEEN IGD.EFFT_D AND IGD.EXPY_D, None, PI1.INT_GRUP_I, PI1.PATY_I = DAP.PATY_I, PI1.PATY_I = DAP.PATY_I AND PI1.REL_C = 'RLMT', PI1.PATY_I = DAP.PATY_I AND PI1.REL_C = 'RLMT' AND :EXTR_D BETWEEN PI1.EFFT_D AND PI1.EXPY_D, REVERSE(PI1.INT_GRUP_I), SEGM_PRTY.BUSN_SEGM_PRTY_N, SEGM_PRTY.DEPT_NODE_C = 'NDEPT902744', SEGM_PRTY.DEPT_NODE_C = 'NDEPT902744' AND SUBSTRING(REVERSE(PI1.INT_GRUP_I), 3, 1) = 'B'
- **Window_Functions**: None

## Migration Recommendations

### Suggested Migration Strategy
**High complexity** - Break into multiple models, use DCF full framework

### DCF Mapping
- **Error Handling**: Convert `.IF ERRORCODE` patterns to DCF error handling macros
- **File Operations**: Replace IMPORT/EXPORT with dbt seeds or external tables
- **Statistics**: Replace COLLECT STATS with Snowflake post-hooks
- **Stored Procedures**: Map CALL statements to DCF stored procedure macros

---

*Generated by BTEQ Parser Service - 2025-08-20 12:39:18*
