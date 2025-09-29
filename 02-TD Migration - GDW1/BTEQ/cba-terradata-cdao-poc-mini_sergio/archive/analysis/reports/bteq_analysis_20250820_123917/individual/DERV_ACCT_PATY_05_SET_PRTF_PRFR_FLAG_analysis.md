# DERV_ACCT_PATY_05_SET_PRTF_PRFR_FLAG.sql - BTEQ Analysis

## File Overview
- **File Name**: DERV_ACCT_PATY_05_SET_PRTF_PRFR_FLAG.sql
- **Analysis Status**: ✅ Success
- **Control Statements**: 21
- **SQL Blocks**: 6

## Control Flow Analysis

| Line | Type | Statement |
|------|------|-----------|
| 1 | RUN | `.RUN FILE=%%BTEQ_LOGON_SCRIPT%%` |
| 42 | IF_ERRORCODE | `.IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 44 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 63 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 65 | COLLECT_STATS | ` COLLECT STATS %%DDSTG%%.DERV_PRTF_ACCT_PATY_PSST;` |
| 66 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 79 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 154 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 156 | COLLECT_STATS | ` COLLECT STATS %%DDSTG%%.DERV_PRTF_ACCT_PATY_STAG;` |
| 157 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 213 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 215 | COLLECT_STATS | ` COLLECT STATS %%DDSTG%%.DERV_PRTF_ACCT_PATY_STAG;` |
| 216 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 227 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 229 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 250 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 252 | COLLECT_STATS | `COLLECT STATS    %%DDSTG%%.DERV_ACCT_PATY_RM;` |
| 253 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 259 | IMPORT | `.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt` |
| 290 | IF_ERRORCODE | ` .IF ERRORCODE   <> 0 THEN .GOTO EXITERR` |
| 300 | LABEL | `.LABEL EXITERR` |

## SQL Blocks Analysis

### SQL Block 1 (Lines 40-41)
- **Complexity Score**: 4
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
DELETE FROM %%DDSTG%%.DERV_PRTF_ACCT_PATY_PSST;
```

#### ❄️ Converted Snowflake SQL:
```sql
DELETE FROM %%DDSTG%%.DERV_PRTF_ACCT_PATY_PSST
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
DELETE FROM "%%ddstg%%"."derv_prtf_acct_paty_psst"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_ACCT_PATY_PSST

### SQL Block 2 (Lines 47-62)
- **Complexity Score**: 75
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_PRTF_ACCT_PATY_PSST
 SEL DAP.ASSC_ACCT_I
   , DAP.PATY_I
   , DPAS.PRTF_CODE_X AS ACCT_PRTF_c
   , 99 AS RANK_I  	 	   /* C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder  Change  */			 
   , DAP.PATY_ACCT_REL_C	   /* C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder  Change  */
    FROM %%DDSTG%%.DERV_ACCT_PATY_CURR DAP
 JOIN %%DDSTG%%.GRD_GNRC_MAP_DERV_PATY_HOLD AHR
   ON DAP.PATY_ACCT_REL_C = AHR.PATY_ACCT_REL_C
   
   JOIN %%DDSTG%%.DERV_PRTF_ACCT_STAG DPAS
ON DAP.ASSC_ACCT_I = DPAS.ACCT_I
WHERE DPAS.PRTF_CODE_X <> 'NA'
 AND :EXTR_D BETWEEN DAP.EFFT_D AND DAP.EXPY_D
 
 GROUP BY 1,2,3,4,5;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_PRTF_ACCT_PATY_PSST SELECT DAP.ASSC_ACCT_I, DAP.PATY_I, DPAS.PRTF_CODE_X AS ACCT_PRTF_c, 99 AS RANK_I, DAP.PATY_ACCT_REL_C FROM %%DDSTG%%.DERV_ACCT_PATY_CURR AS DAP JOIN %%DDSTG%%.GRD_GNRC_MAP_DERV_PATY_HOLD AS AHR ON DAP.PATY_ACCT_REL_C = AHR.PATY_ACCT_REL_C JOIN %%DDSTG%%.DERV_PRTF_ACCT_STAG AS DPAS ON DAP.ASSC_ACCT_I = DPAS.ACCT_I WHERE DPAS.PRTF_CODE_X <> 'NA' AND :EXTR_D BETWEEN DAP.EFFT_D AND DAP.EXPY_D GROUP BY 1, 2, 3, 4, 5
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."derv_prtf_acct_paty_psst"
SELECT
  "dap"."assc_acct_i" AS "assc_acct_i",
  "dap"."paty_i" AS "paty_i",
  "dpas"."prtf_code_x" AS "acct_prtf_c",
  99 AS "rank_i",
  "dap"."paty_acct_rel_c" AS "paty_acct_rel_c"
FROM "%%ddstg%%"."derv_acct_paty_curr" AS "dap"
JOIN "%%ddstg%%"."grd_gnrc_map_derv_paty_hold" AS "ahr"
  ON "ahr"."paty_acct_rel_c" = "dap"."paty_acct_rel_c"
JOIN "%%ddstg%%"."derv_prtf_acct_stag" AS "dpas"
  ON "dap"."assc_acct_i" = "dpas"."acct_i" AND "dpas"."prtf_code_x" <> 'NA'
WHERE
  "dap"."efft_d" <= :EXTR_D AND "dap"."expy_d" >= :EXTR_D
GROUP BY
  "dap"."assc_acct_i",
  "dap"."paty_i",
  "dpas"."prtf_code_x",
  4,
  "dap"."paty_acct_rel_c"
```

#### 📊 SQL Metadata:
- **Tables**: DERV_ACCT_PATY_CURR, DERV_PRTF_ACCT_PATY_PSST, DERV_PRTF_ACCT_STAG, GRD_GNRC_MAP_DERV_PATY_HOLD
- **Columns**: ACCT_I, ASSC_ACCT_I, EFFT_D, EXPY_D, PATY_ACCT_REL_C, PATY_I, PRTF_CODE_X
- **Functions**: DPAS.PRTF_CODE_X <> 'NA'

### SQL Block 3 (Lines 82-153)
- **Complexity Score**: 0
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ❌ Invalid
- **Teradata Features**: 3

#### 📝 Original Teradata SQL:
```sql
UPDATE  DPAPP
    FROM  %%DDSTG%%.DERV_PRTF_ACCT_PATY_PSST  DPAPP,
	  (SELECT DISTINCT  PIG.PATY_I			  			  
 	       	     ,GDFVC.PRTY
  	      	       FROM  PVDATA.PATY_INT_GRUP_CURR PIG 				
	        INNER JOIN  PVDATA.INT_GRUP_DEPT_CURR IGD 				
  		  	  ON  IGD.INT_GRUP_I=PIG.INT_GRUP_I			 						   
			 AND  PIG.REL_C = 'RLMT'
		 INNER JOIN (  SELECT   GFC.DEPT_LEAF_NODE_C AS DEPT_I
		       		  ,ORU.PRTY PRTY 
	   		 	    FROM   %%VTECH%%.GRD_DEPT_FLAT_CURR  GFC 
	   		      LEFT JOIN  (	   SELECT  COALESCE(PRTY,9999) PRTY 
	      						   ,LKUP1_TEXT
		      					   ,COALESCE(UPDT_DTTS,CRAT_DTTS) LoadTimeStamp
		   		  		     FROM  %%VTECH%%.ODS_RULE --%%VTECH%% Should be replaced by 
 				 		    WHERE  RULE_CODE = 'RMPOC'			
	 		  		             AND :EXTR_D BETWEEN VALD_FROM AND VALD_TO
		 	       		  QUALIFY  ROW_NUMBER() OVER (PARTITION BY LKUP1_TEXT,PRTY ORDER BY LoadTimeStamp DESC) = 1		
 		 	     		    )ORU
			             ON   GFC.DEPT_L3_NODE_C = ORU.LKUP1_TEXT
	   		          WHERE   PRTY <> 9999 
			      )GDFVC
	  	         ON  GDFVC.DEPT_I = IGD.DEPT_I
  		      WHERE  PIG.PATY_I IN (SELECT 
			                   DISTINCT PATY_I 
				     	  	  FROM %%DDSTG%%.DERV_PRTF_ACCT_PATY_PSST)		     			    	 	 
		    QUALIFY  ROW_NUMBER() OVER (PARTITION BY PIG.PATY_I ORDER BY GDFVC.PRTY ASC ) = 1
		 )DRVD  
     SET  RANK_I = DRVD.PRTY			   
   WHERE  DPAPP.PATY_I= DRVD.PATY_I
     AND  DPAPP.PATY_ACCT_REL_C= 'ZINTE0';
/*
	End - C2039849_IM1317265_Revenue_Attribution_to_Fund_Holder  	
*/

-- Insert one row per account with the highest value of
-- PATY_I if there's more than one party.

DELETE FROM %%DDSTG%%.DERV_PRTF_ACCT_PATY_STAG;

/* 
	Commented the Below Code against the CR C2039849_IM1317265_Revenue_Attribution_to_Fund_Holder  

*/

 
 
 
 
/*
-- Insert one row per account with the highest value of
-- PATY_I if there's more than one party.





DELETE FROM %%DDSTG%%.DERV_PRTF_ACCT_PATY_STAG;

INSERT INTO %%DDSTG%%.DERV_PRTF_ACCT_PATY_STAG
 SEL DAPP.ACCT_I
   , DAPP.PATY_I
   , DAPP.ACCT_PRTF_C
   , DPPS.PRTF_CODE_X AS PATY_PRTF_C 
 FROM %%DDSTG%%.DERV_PRTF_ACCT_PATY_PSST DAPP
 JOIN %%DDSTG%%.DERV_PRTF_PATY_STAG DPPS
   ON DAPP.PATY_I = DPPS.PATY_I
  AND DAPP.ACCT_PRTF_C  = DPPS.PRTF_CODE_X

WHERE  DPPS.PRTF_CODE_X <> 'NA'
QUALIFY ROW_NUMBER() OVER (PARTITION BY DAPP.ACCT_I ORDER BY DAPP.PATY_I DESC) = 1;
```

#### ❄️ Converted Snowflake SQL:
```sql
UPDATE  DPAPP
    FROM  %%DDSTG%%.DERV_PRTF_ACCT_PATY_PSST  DPAPP,
	  (SELECT DISTINCT  PIG.PATY_I			  			  
 	       	     ,GDFVC.PRTY
  	      	       FROM  PVDATA.PATY_INT_GRUP_CURR PIG 				
	        INNER JOIN  PVDATA.INT_GRUP_DEPT_CURR IGD 				
  		  	  ON  IGD.INT_GRUP_I=PIG.INT_GRUP_I			 						   
			 AND  PIG.REL_C = 'RLMT'
		 INNER JOIN (  SELECT   GFC.DEPT_LEAF_NODE_C AS DEPT_I
		       		  ,ORU.PRTY PRTY 
	   		 	    FROM   %%VTECH%%.GRD_DEPT_FLAT_CURR  GFC 
	   		      LEFT JOIN  (	   SELECT  COALESCE(PRTY,9999) PRTY 
	      						   ,LKUP1_TEXT
		      					   ,COALESCE(UPDT_DTTS,CRAT_DTTS) LoadTimeStamp
		   		  		     FROM  %%VTECH%%.ODS_RULE --%%VTECH%% Should be replaced by 
 				 		    WHERE  RULE_CODE = 'RMPOC'			
	 		  		             AND :EXTR_D BETWEEN VALD_FROM AND VALD_TO
		 	       		  QUALIFY  ROW_NUMBER() OVER (PARTITION BY LKUP1_TEXT,PRTY ORDER BY LoadTimeStamp DESC) = 1		
 		 	     		    )ORU
			             ON   GFC.DEPT_L3_NODE_C = ORU.LKUP1_TEXT
	   		          WHERE   PRTY <> 9999 
			      )GDFVC
	  	         ON  GDFVC.DEPT_I = IGD.DEPT_I
  		      WHERE  PIG.PATY_I IN (SELECT 
			                   DISTINCT PATY_I 
				     	  	  FROM %%DDSTG%%.DERV_PRTF_ACCT_PATY_PSST)		     			    	 	 
		    QUALIFY  ROW_NUMBER() OVER (PARTITION BY PIG.PATY_I ORDER BY GDFVC.PRTY ASC ) = 1
		 )DRVD  
     SET  RANK_I = DRVD.PRTY			   
   WHERE  DPAPP.PATY_I= DRVD.PATY_I
     AND  DPAPP.PATY_ACCT_REL_C= 'ZINTE0';
/*
	End - C2039849_IM1317265_Revenue_Attribution_to_Fund_Holder  	
*/

-- Insert one row per account with the highest value of
-- PATY_I if there's more than one party.

DELETE FROM %%DDSTG%%.DERV_PRTF_ACCT_PATY_STAG;

/* 
	Commented the Below Code against the CR C2039849_IM1317265_Revenue_Attribution_to_Fund_Holder  

*/

 
 
 
 
/*
-- Insert one row per account with the highest value of
-- PATY_I if there's more than one party.





DELETE FROM %%DDSTG%%.DERV_PRTF_ACCT_PATY_STAG;

INSERT INTO %%DDSTG%%.DERV_PRTF_ACCT_PATY_STAG
 SEL DAPP.ACCT_I
   , DAPP.PATY_I
   , DAPP.ACCT_PRTF_C
   , DPPS.PRTF_CODE_X AS PATY_PRTF_C 
 FROM %%DDSTG%%.DERV_PRTF_ACCT_PATY_PSST DAPP
 JOIN %%DDSTG%%.DERV_PRTF_PATY_STAG DPPS
   ON DAPP.PATY_I = DPPS.PATY_I
  AND DAPP.ACCT_PRTF_C  = DPPS.PRTF_CODE_X

WHERE  DPPS.PRTF_CODE_X <> 'NA'
QUALIFY ROW_NUMBER() OVER (PARTITION BY DAPP.ACCT_I ORDER BY DAPP.PATY_I DESC) = 1;
```

#### 🔍 Syntax Validation Details:
- **Valid**: ❌

#### 🎯 Teradata Features Detected:
- QUALIFY clause
- ROW_NUMBER() OVER
- Variable substitution

### SQL Block 4 (Lines 175-212)
- **Complexity Score**: 87
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 3

#### 📝 Original Teradata SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_PRTF_ACCT_PATY_STAG
 SEL DAPP.ACCT_I						
   , DAPP.PATY_I
   , DAPP.ACCT_PRTF_C
   , DPPS.PRTF_CODE_X AS PATY_PRTF_C
   , DAPP.PATY_ACCT_REL_C 
   , CASE 
          WHEN DAPP.RANK_I <> 99 AND DAPP.PATY_ACCT_REL_C = 'ZINTE0'  /* Multiple Fund Holder Scenarios Handling */         
	   THEN DAPP.RANK_I 						 
   	   WHEN PATY_ACCT_REL_C = 'ZINTE0'  		        	/* Simple Account with FundHolder Accounts No Multiple Scenarios */
   	   THEN 98
	   ELSE 99				 		        	/* Least Priority With No Fund Holders Relationship */
      END RANK_I   
 FROM %%DDSTG%%.DERV_PRTF_ACCT_PATY_PSST DAPP
 JOIN %%DDSTG%%.DERV_PRTF_PATY_STAG DPPS
   ON DAPP.PATY_I = DPPS.PATY_I
AND (DPPS.PRTF_CODE_X=DAPP.ACCT_PRTF_C or DAPP.PATY_ACCT_REL_C = 'ZINTE0')
  AND DPPS.PRTF_CODE_X <> 'NA'          
;

/* The Below Code Filters or Deletes the Records which are having More than 1 Accounts & Muliple relationships based on PATY_I & MIN(Rank)   */

DELETE FROM  %%DDSTG%%.DERV_PRTF_ACCT_PATY_STAG DPAPS 
WHERE (ACCT_I,RANK_I,PATY_I) IN (   SELECT ACCT_I,RANK_I,PATY_I FROM 	
									(SELECT  ACCT_I 
										   ,RANK_I
										   ,PATY_I	   
									  FROM  %%DDSTG%%.DERV_PRTF_ACCT_PATY_STAG DPAPS
									QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCT_I ORDER BY RANK_I ASC,PATY_I DESC) > 1
									)DRVD
								);


/* END    C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder  Change  */
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_PRTF_ACCT_PATY_STAG SELECT DAPP.ACCT_I, DAPP.PATY_I, DAPP.ACCT_PRTF_C, DPPS.PRTF_CODE_X AS PATY_PRTF_C, DAPP.PATY_ACCT_REL_C, CASE WHEN DAPP.RANK_I <> 99 AND DAPP.PATY_ACCT_REL_C = 'ZINTE0' THEN DAPP.RANK_I WHEN PATY_ACCT_REL_C = 'ZINTE0' THEN 98 ELSE 99 END AS RANK_I FROM %%DDSTG%%.DERV_PRTF_ACCT_PATY_PSST AS DAPP JOIN %%DDSTG%%.DERV_PRTF_PATY_STAG AS DPPS ON DAPP.PATY_I = DPPS.PATY_I AND (DPPS.PRTF_CODE_X = DAPP.ACCT_PRTF_C OR DAPP.PATY_ACCT_REL_C = 'ZINTE0') AND DPPS.PRTF_CODE_X <> 'NA'
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- QUALIFY clause
- ROW_NUMBER() OVER
- Variable substitution

#### 📊 SQL Metadata:
- **Tables**: DERV_PRTF_ACCT_PATY_PSST, DERV_PRTF_ACCT_PATY_STAG, DERV_PRTF_PATY_STAG
- **Columns**: ACCT_I, ACCT_PRTF_C, PATY_ACCT_REL_C, PATY_I, PRTF_CODE_X, RANK_I
- **Functions**: DAPP.PATY_I = DPPS.PATY_I, DAPP.PATY_I = DPPS.PATY_I AND (DPPS.PRTF_CODE_X = DAPP.ACCT_PRTF_C OR DAPP.PATY_ACCT_REL_C = 'ZINTE0'), DAPP.RANK_I <> 99, DAPP.RANK_I <> 99 AND DAPP.PATY_ACCT_REL_C = 'ZINTE0', DPPS.PRTF_CODE_X = DAPP.ACCT_PRTF_C, None, PATY_ACCT_REL_C = 'ZINTE0'

### SQL Block 5 (Lines 232-249)
- **Complexity Score**: 60
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 1

#### 📝 Original Teradata SQL:
```sql
INSERT INTO  %%DDSTG%%.DERV_ACCT_PATY_RM

SELECT 
 DAP.ACCT_I
      ,DAP.PATY_I
      ,DAP.ASSC_ACCT_I
      ,DAP.PATY_ACCT_REL_C
      ,DAP.PRFR_PATY_F
     ,DAP.SRCE_SYST_C
      ,DAP.EFFT_D
      ,DAP.EXPY_D
      ,DAP.ROW_SECU_ACCS_C
FROM %%DDSTG%%.DERV_ACCT_PATY_CURR DAP
JOIN %%DDSTG%%.DERV_PRTF_ACCT_PATY_STAG T1

  ON DAP.ASSC_ACCT_I = T1.ACCT_I
  WHERE :EXTR_D BETWEEN DAP.EFFT_D AND DAP.EXPY_D;
```

#### ❄️ Converted Snowflake SQL:
```sql
INSERT INTO %%DDSTG%%.DERV_ACCT_PATY_RM SELECT DAP.ACCT_I, DAP.PATY_I, DAP.ASSC_ACCT_I, DAP.PATY_ACCT_REL_C, DAP.PRFR_PATY_F, DAP.SRCE_SYST_C, DAP.EFFT_D, DAP.EXPY_D, DAP.ROW_SECU_ACCS_C FROM %%DDSTG%%.DERV_ACCT_PATY_CURR AS DAP JOIN %%DDSTG%%.DERV_PRTF_ACCT_PATY_STAG AS T1 ON DAP.ASSC_ACCT_I = T1.ACCT_I WHERE :EXTR_D BETWEEN DAP.EFFT_D AND DAP.EXPY_D
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- Variable substitution

#### ⚡ Optimized SQL:
```sql
INSERT INTO "%%ddstg%%"."derv_acct_paty_rm"
SELECT
  "dap"."acct_i" AS "acct_i",
  "dap"."paty_i" AS "paty_i",
  "dap"."assc_acct_i" AS "assc_acct_i",
  "dap"."paty_acct_rel_c" AS "paty_acct_rel_c",
  "dap"."prfr_paty_f" AS "prfr_paty_f",
  "dap"."srce_syst_c" AS "srce_syst_c",
  "dap"."efft_d" AS "efft_d",
  "dap"."expy_d" AS "expy_d",
  "dap"."row_secu_accs_c" AS "row_secu_accs_c"
FROM "%%ddstg%%"."derv_acct_paty_curr" AS "dap"
JOIN "%%ddstg%%"."derv_prtf_acct_paty_stag" AS "t1"
  ON "dap"."assc_acct_i" = "t1"."acct_i"
WHERE
  "dap"."efft_d" <= :EXTR_D AND "dap"."expy_d" >= :EXTR_D
```

#### 📊 SQL Metadata:
- **Tables**: DERV_ACCT_PATY_CURR, DERV_ACCT_PATY_RM, DERV_PRTF_ACCT_PATY_STAG
- **Columns**: ACCT_I, ASSC_ACCT_I, EFFT_D, EXPY_D, PATY_ACCT_REL_C, PATY_I, PRFR_PATY_F, ROW_SECU_ACCS_C, SRCE_SYST_C

### SQL Block 6 (Lines 262-289)
- **Complexity Score**: 155
- **Has Valid SQL**: ✅
- **Conversion Successful**: ✅
- **Syntax Validation**: ✅ Valid
- **Teradata Features**: 3

#### 📝 Original Teradata SQL:
```sql
UPDATE DAP
FROM %%DDSTG%%.DERV_ACCT_PATY_RM DAP,
(SEL T1.ACCT_I
      
      ,T1.PATY_I
      ,T1.PATY_aCCT_REL_C
      ,T1.EFFT_D
      ,T1.EXPY_D
       FROM %%DDSTG%%.DERV_ACCT_PATY_RM T1
       JOIN %%DDSTG%%.GRD_GNRC_MAP_DERV_PATY_HOLD T2
  
  ON T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
       
       JOIN %%DDSTG%%.DERV_PRTF_ACCT_PATY_STAG T4
       ON T1.ASSC_ACCT_I = T4.ACCT_I
       AND T1.PATY_I = T4.PATY_I
	   AND T1.PATY_ACCT_REL_C = T4.PATY_ACCT_REL_C   /* 1. C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder Change 2. Added the missing Condition  */

   WHERE :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_d
/*QUALIFY ROW_NUMBER() OVER (PARTITION BY T1.ACCT_I ORDER BY T1.EFFT_D, T1.PATY_ACCT_REL_C) = 1) T3 */ /* C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder Change */
QUALIFY ROW_NUMBER() OVER (PARTITION BY T1.ACCT_I ORDER BY T4.RANK_I,T1.EFFT_D,T1.PATY_ACCT_REL_C,T4.PATY_I DESC) = 1) T3   /*C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder  Change  */

SET PRFR_PATY_F = 'Y'
WHERE DAP.ACCT_I = T3.ACCT_I
AND DAP.PATY_I = T3.PATY_I
AND DAP.PATY_ACCT_REL_C = T3.PATY_ACCT_REL_C
AND DAP.EFFT_D = T3.EFFT_D
AND DAP.EXPY_D = T3.EXPY_D;
```

#### ❄️ Converted Snowflake SQL:
```sql
UPDATE DAP SET PRFR_PATY_F = 'Y' FROM %%DDSTG%%.DERV_ACCT_PATY_RM AS DAP, (SELECT T1.ACCT_I, T1.PATY_I, T1.PATY_aCCT_REL_C, T1.EFFT_D, T1.EXPY_D FROM %%DDSTG%%.DERV_ACCT_PATY_RM AS T1 JOIN %%DDSTG%%.GRD_GNRC_MAP_DERV_PATY_HOLD AS T2 ON T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C JOIN %%DDSTG%%.DERV_PRTF_ACCT_PATY_STAG AS T4 ON T1.ASSC_ACCT_I = T4.ACCT_I AND T1.PATY_I = T4.PATY_I AND T1.PATY_ACCT_REL_C = T4.PATY_ACCT_REL_C WHERE :EXTR_D BETWEEN T1.EFFT_D AND T1.EXPY_d QUALIFY ROW_NUMBER() OVER (PARTITION BY T1.ACCT_I ORDER BY T4.RANK_I NULLS FIRST, T1.EFFT_D NULLS FIRST, T1.PATY_ACCT_REL_C NULLS FIRST, T4.PATY_I DESC NULLS LAST) = 1) AS T3 WHERE DAP.ACCT_I = T3.ACCT_I AND DAP.PATY_I = T3.PATY_I AND DAP.PATY_ACCT_REL_C = T3.PATY_ACCT_REL_C AND DAP.EFFT_D = T3.EFFT_D AND DAP.EXPY_D = T3.EXPY_D
```

#### 🔍 Syntax Validation Details:
- **Valid**: ✅

#### 🎯 Teradata Features Detected:
- QUALIFY clause
- ROW_NUMBER() OVER
- Variable substitution

#### ⚡ Optimized SQL:
```sql
UPDATE "dap" SET "prfr_paty_f" = 'Y'
FROM "%%ddstg%%"."derv_acct_paty_rm" AS "dap"
  CROSS JOIN (
    SELECT
      "t1"."acct_i" AS "acct_i",
      "t1"."paty_i" AS "paty_i",
      "t1"."paty_acct_rel_c" AS "paty_acct_rel_c",
      "t1"."efft_d" AS "efft_d",
      "t1"."expy_d" AS "expy_d"
    FROM "%%ddstg%%"."derv_acct_paty_rm" AS "t1"
    JOIN "%%ddstg%%"."grd_gnrc_map_derv_paty_hold" AS "t2"
      ON "t1"."paty_acct_rel_c" = "t2"."paty_acct_rel_c"
    JOIN "%%ddstg%%"."derv_prtf_acct_paty_stag" AS "t4"
      ON "t1"."assc_acct_i" = "t4"."acct_i"
      AND "t1"."paty_acct_rel_c" = "t4"."paty_acct_rel_c"
      AND "t1"."paty_i" = "t4"."paty_i"
    WHERE
      "t1"."efft_d" <= :EXTR_D AND "t1"."expy_d" >= :EXTR_D
    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY "t1"."acct_i"
        ORDER BY "t4"."rank_i" NULLS FIRST, "t1"."efft_d" NULLS FIRST, "t1"."paty_acct_rel_c" NULLS FIRST, "t4"."paty_i" DESC NULLS LAST
      ) = 1
  ) AS "t3"
WHERE
  "dap"."acct_i" = "t3"."acct_i"
  AND "dap"."efft_d" = "t3"."efft_d"
  AND "dap"."expy_d" = "t3"."expy_d"
  AND "dap"."paty_acct_rel_c" = "t3"."paty_acct_rel_c"
  AND "dap"."paty_i" = "t3"."paty_i"
```

#### 📊 SQL Metadata:
- **Tables**: DAP, DERV_ACCT_PATY_RM, DERV_PRTF_ACCT_PATY_STAG, GRD_GNRC_MAP_DERV_PATY_HOLD
- **Columns**: ACCT_I, ASSC_ACCT_I, EFFT_D, EXPY_D, EXPY_d, PATY_ACCT_REL_C, PATY_I, PATY_aCCT_REL_C, PRFR_PATY_F, RANK_I
- **Functions**: DAP.ACCT_I = T3.ACCT_I, DAP.ACCT_I = T3.ACCT_I AND DAP.PATY_I = T3.PATY_I, DAP.ACCT_I = T3.ACCT_I AND DAP.PATY_I = T3.PATY_I AND DAP.PATY_ACCT_REL_C = T3.PATY_ACCT_REL_C, DAP.ACCT_I = T3.ACCT_I AND DAP.PATY_I = T3.PATY_I AND DAP.PATY_ACCT_REL_C = T3.PATY_ACCT_REL_C AND DAP.EFFT_D = T3.EFFT_D, None, T1.ASSC_ACCT_I = T4.ACCT_I, T1.ASSC_ACCT_I = T4.ACCT_I AND T1.PATY_I = T4.PATY_I
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

*Generated by BTEQ Parser Service - 2025-08-20 12:39:25*
