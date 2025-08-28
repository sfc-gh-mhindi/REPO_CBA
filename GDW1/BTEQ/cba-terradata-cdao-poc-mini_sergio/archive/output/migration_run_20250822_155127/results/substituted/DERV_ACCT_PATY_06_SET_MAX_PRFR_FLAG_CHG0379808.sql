.RUN FILE=%%BTEQ_LOGON_SCRIPT%%

.IF ERRORLEVEL <> 0 THEN .GOTO EXITERR

.SET QUIET OFF
.SET ECHOREQ ON
.SET FORMAT OFF
.SET WIDTH 120

------------------------------------------------------------------------------
-- Object Name             :  DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG.sql
-- Object Type             :  BTEQ

--                           
-- Description             :  set preferred party flag for non-RM accounts  
--                            Use MAX(PATY_I) for this purpose and remove holder rows
--                            that have effective date prior to the effective date of the 
--                            flagged row. 
------------------------------------------------------------------------------
-- Modification History
-- Date               Author           Version     Version Description
-- 04/06/2013         Helen Zak        1.0         Initial Version
-- 08/08/2013         Helen Zak        1.1         C0714578- post-implementatino fix
--                                                 Use persisted GRD tables for better performance
-- 22/08/2013        Helen Zak        1.1          C0726912 - post-implementation fix
--                                                 Include missed error handling
-- 02/09/2013        Helen Zak        1.2         C0737261
--                                                 Correct setting of PRFR_PATY_F
-- 25/08/2016        A&IPlatformRRTSquad@cba.com.au
--				      					1.3          C2039845
--                                                 DERV_ACCT_PATY PRTF_PATY_F Update for FUND HOLDER 
------------------------------------------------------------------------------
   -- Identify non-RM managed accounts and insert them into a separate table
 
   
DELETE FROM %%DDSTG%%.DERV_ACCT_PATY_NON_RM ALL;

.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
USING 
( EXTR_D VARCHAR(10) )  
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

   .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
   
   COLLECT STATS    %%DDSTG%%.DERV_ACCT_PATY_NON_RM;
 .IF ERRORCODE   <> 0 THEN .GOTO EXITERR

 
 
 
 
/* 
	BEGIN  C2039849_IM1317265_Revenue_Attribution_to_Fund_Holder for Non RM Accounts 
	1. The Below Logic gets the priority to be set for all 
	   the Customers having Multiple Fund holders relationship 
	2. This uses ODS Table Called ODS_RULE to understand the Priority List 	
	
*/
.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
USING 
( EXTR_D VARCHAR(10) )  
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

.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
USING 
( EXTR_D VARCHAR(10) )   
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

 .IF ERRORCODE   <> 0 THEN .GOTO EXITERR



-- Insert rows for the accounts for which the flag was set (both RM and non-RM).
-- Use fast path for efficiency.


DEL FROM %%DDSTG%%.DERV_ACCT_PATY_FLAG ALL;
 .IF ERRORCODE   <> 0 THEN .GOTO EXITERR



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

.IF ERRORCODE   <> 0 THEN .GOTO EXITERR 

COLLECT STATS %%DDSTG%%.DERV_ACCT_PATY_FLAG;

.IF ERRORCODE   <> 0 THEN .GOTO EXITERR 

.QUIT 0
-- $LastChangedBy: zakhe $
-- $LastChangedDate: 2013-09-04 15:23:55 +1000 (Wed, 04 Sep 2013) $
-- $LastChangedRevision: 12590 $

.LABEL EXITERR
.QUIT 4
