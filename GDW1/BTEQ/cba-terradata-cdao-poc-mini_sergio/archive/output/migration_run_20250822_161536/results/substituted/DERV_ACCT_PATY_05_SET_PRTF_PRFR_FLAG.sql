.RUN FILE=${ETL_APP_BTEQ}/bteq_login.sql

.IF ERRORLEVEL <> 0 THEN .GOTO EXITERR

.SET QUIET OFF
.SET ECHOREQ ON
.SET FORMAT OFF
.SET WIDTH 120

------------------------------------------------------------------------------
-- Object Name             :  DERV_ACCT_PATY_05_SET_PRTF_PRFR_FLAG.sql
-- Object Type             :  BTEQ

--                           
-- Description             :  set preferred party flag for the accounts that
--                                      have the same RM for acct and a party
--                          
------------------------------------------------------------------------------
-- Modification History
-- Date               Author           Version     Version Description
-- 04/06/2013         Helen Zak        1.0         Initial Version
-- 08/08/2013         Helen Zak        1.1         C0714578 - post-implementation fixes
--                                                 1. Use GRD persisted tables for better performance
--                                                 2. Correct logic for portfolio managed accounts/parties 
-- 21/08/2013         Helen Zak        1.2         C0726912 - post-implementation fix
--                                                 Another attempt to correct logic for RM accounts/parties
-- 28/08/2013         Helen Zak        1.3         C0733426
--                                                 Extra persisting for better performance    
-- 02/09/2013        Helen Zak         1.4        C0737261
--                                                 If more than one party are managed by the same RM as
--                                                 the account, pick the max PATY_I of those 
-- 25/08/2016        A&IPlatformRRTSquad@cba.com.au
--				      					1.5          C2039845
--                                                 DERV_ACCT_PATY PRTF_PATY_F Update for FUND HOLDER           
------------------------------------------------------------------------------
-- Insert rows from the _CURR table for which we have portfolio
-- information identified


DELETE FROM PDDSTG.DERV_PRTF_ACCT_PATY_PSST;

.IF ERRORCODE   <> 0 THEN .GOTO EXITERR 

.IMPORT VARTEXT FILE=/cba_app/CBMGDW/PROD/schedule/DERV_ACCT_PATY_extr_date.txt
USING 
( EXTR_D VARCHAR(10) )
INSERT INTO PDDSTG.DERV_PRTF_ACCT_PATY_PSST
 SEL DAP.ASSC_ACCT_I
   , DAP.PATY_I
   , DPAS.PRTF_CODE_X AS ACCT_PRTF_c
   , 99 AS RANK_I  	 	   /* C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder  Change  */			 
   , DAP.PATY_ACCT_REL_C	   /* C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder  Change  */
    FROM PDDSTG.DERV_ACCT_PATY_CURR DAP
 JOIN PDDSTG.GRD_GNRC_MAP_DERV_PATY_HOLD AHR
   ON DAP.PATY_ACCT_REL_C = AHR.PATY_ACCT_REL_C
   
   JOIN PDDSTG.DERV_PRTF_ACCT_STAG DPAS
ON DAP.ASSC_ACCT_I = DPAS.ACCT_I
WHERE DPAS.PRTF_CODE_X <> 'NA'
 AND :EXTR_D BETWEEN DAP.EFFT_D AND DAP.EXPY_D
 
 GROUP BY 1,2,3,4,5; 
 .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
 
 COLLECT STATS PDDSTG.DERV_PRTF_ACCT_PATY_PSST;
 .IF ERRORCODE   <> 0 THEN .GOTO EXITERR

 
 
 
 
/* 
	BEGIN  C2039849_IM1317265_Revenue_Attribution_to_Fund_Holder  	
	1. The Below Logic gets the priority to be set for all 
	   the Customers having Multiple Fund holders relationship 
	2. This uses ODS Table Called ODS_RULE to understand the Priority List 	
	
*/
.IMPORT VARTEXT FILE=/cba_app/CBMGDW/PROD/schedule/DERV_ACCT_PATY_extr_date.txt
USING 
( EXTR_D VARCHAR(10) )
   UPDATE  DPAPP
    FROM  PDDSTG.DERV_PRTF_ACCT_PATY_PSST  DPAPP,
	  (SELECT DISTINCT  PIG.PATY_I			  			  
 	       	     ,GDFVC.PRTY
  	      	       FROM  PVDATA.PATY_INT_GRUP_CURR PIG 				
	        INNER JOIN  PVDATA.INT_GRUP_DEPT_CURR IGD 				
  		  	  ON  IGD.INT_GRUP_I=PIG.INT_GRUP_I			 						   
			 AND  PIG.REL_C = 'RLMT'
		 INNER JOIN (  SELECT   GFC.DEPT_LEAF_NODE_C AS DEPT_I
		       		  ,ORU.PRTY PRTY 
	   		 	    FROM   PVTECH.GRD_DEPT_FLAT_CURR  GFC 
	   		      LEFT JOIN  (	   SELECT  COALESCE(PRTY,9999) PRTY 
	      						   ,LKUP1_TEXT
		      					   ,COALESCE(UPDT_DTTS,CRAT_DTTS) LoadTimeStamp
		   		  		     FROM  PVTECH.ODS_RULE --PVTECH Should be replaced by 
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
				     	  	  FROM PDDSTG.DERV_PRTF_ACCT_PATY_PSST)		     			    	 	 
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

DELETE FROM PDDSTG.DERV_PRTF_ACCT_PATY_STAG;

/* 
	Commented the Below Code against the CR C2039849_IM1317265_Revenue_Attribution_to_Fund_Holder  

*/

 
 
 
 
/*
-- Insert one row per account with the highest value of
-- PATY_I if there's more than one party.





DELETE FROM PDDSTG.DERV_PRTF_ACCT_PATY_STAG;

INSERT INTO PDDSTG.DERV_PRTF_ACCT_PATY_STAG
 SEL DAPP.ACCT_I
   , DAPP.PATY_I
   , DAPP.ACCT_PRTF_C
   , DPPS.PRTF_CODE_X AS PATY_PRTF_C 
 FROM PDDSTG.DERV_PRTF_ACCT_PATY_PSST DAPP
 JOIN PDDSTG.DERV_PRTF_PATY_STAG DPPS
   ON DAPP.PATY_I = DPPS.PATY_I
  AND DAPP.ACCT_PRTF_C  = DPPS.PRTF_CODE_X

WHERE  DPPS.PRTF_CODE_X <> 'NA'
QUALIFY ROW_NUMBER() OVER (PARTITION BY DAPP.ACCT_I ORDER BY DAPP.PATY_I DESC) = 1;

 .IF ERRORCODE   <> 0 THEN .GOTO EXITERR 
 
 COLLECT STATS PDDSTG.DERV_PRTF_ACCT_PATY_STAG;
 .IF ERRORCODE   <> 0 THEN .GOTO EXITERR 

 */
 
 
 
/* 
	BEGIN   C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder  Change  
	1. Commented the Above code & 
	2. Re-Wrote the Code with Rank & Priority For FundHolder 
	3. Rules Remain the Same Except 
		a. ZINTE0 - FunHolder Accounts gets the First Priority for Preferred Party Flag 
		b. ZINTE0 - Parties Having Multiple  Fund Holder woudl be Prioritised based on the ODS Table
		

*/			 


INSERT INTO PDDSTG.DERV_PRTF_ACCT_PATY_STAG
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
 FROM PDDSTG.DERV_PRTF_ACCT_PATY_PSST DAPP
 JOIN PDDSTG.DERV_PRTF_PATY_STAG DPPS
   ON DAPP.PATY_I = DPPS.PATY_I
AND (DPPS.PRTF_CODE_X=DAPP.ACCT_PRTF_C or DAPP.PATY_ACCT_REL_C = 'ZINTE0')
  AND DPPS.PRTF_CODE_X <> 'NA'          
;

/* The Below Code Filters or Deletes the Records which are having More than 1 Accounts & Muliple relationships based on PATY_I & MIN(Rank)   */

DELETE FROM  PDDSTG.DERV_PRTF_ACCT_PATY_STAG DPAPS 
WHERE (ACCT_I,RANK_I,PATY_I) IN (   SELECT ACCT_I,RANK_I,PATY_I FROM 	
									(SELECT  ACCT_I 
										   ,RANK_I
										   ,PATY_I	   
									  FROM  PDDSTG.DERV_PRTF_ACCT_PATY_STAG DPAPS
									QUALIFY ROW_NUMBER() OVER (PARTITION BY ACCT_I ORDER BY RANK_I ASC,PATY_I DESC) > 1
									)DRVD
								);


/* END    C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder  Change  */			 




 .IF ERRORCODE   <> 0 THEN .GOTO EXITERR 
 
 COLLECT STATS PDDSTG.DERV_PRTF_ACCT_PATY_STAG;
 .IF ERRORCODE   <> 0 THEN .GOTO EXITERR 
 

 

 
--Insert rows for the accounts that have portfolio info identified
-- and at least one party is managed by the same RM as the account this
-- party is attached to. 

DEL FROM PDDSTG.DERV_ACCT_PATY_RM ALL;
 .IF ERRORCODE   <> 0 THEN .GOTO EXITERR

.IMPORT VARTEXT FILE=/cba_app/CBMGDW/PROD/schedule/DERV_ACCT_PATY_extr_date.txt
USING 
( EXTR_D VARCHAR(10) ) 
INSERT INTO  PDDSTG.DERV_ACCT_PATY_RM

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
FROM PDDSTG.DERV_ACCT_PATY_CURR DAP
JOIN PDDSTG.DERV_PRTF_ACCT_PATY_STAG T1

  ON DAP.ASSC_ACCT_I = T1.ACCT_I
  WHERE :EXTR_D BETWEEN DAP.EFFT_D AND DAP.EXPY_D;

 .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
        
COLLECT STATS    PDDSTG.DERV_ACCT_PATY_RM;
 .IF ERRORCODE   <> 0 THEN .GOTO EXITERR
 

-- Set preferred party flag for acct_i/Paty_i combination for the identified acct_i/Paty_i
-- for holder relationships

.IMPORT VARTEXT FILE=/cba_app/CBMGDW/PROD/schedule/DERV_ACCT_PATY_extr_date.txt
USING 
( EXTR_D VARCHAR(10) )   
UPDATE DAP
FROM PDDSTG.DERV_ACCT_PATY_RM DAP,
(SEL T1.ACCT_I
      
      ,T1.PATY_I
      ,T1.PATY_aCCT_REL_C
      ,T1.EFFT_D
      ,T1.EXPY_D
       FROM PDDSTG.DERV_ACCT_PATY_RM T1
       JOIN PDDSTG.GRD_GNRC_MAP_DERV_PATY_HOLD T2
  
  ON T1.PATY_ACCT_REL_C = T2.PATY_ACCT_REL_C
       
       JOIN PDDSTG.DERV_PRTF_ACCT_PATY_STAG T4
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
 .IF ERRORCODE   <> 0 THEN .GOTO EXITERR



.QUIT 0

-- $LastChangedBy: zakhe $
-- $LastChangedDate: 2013-09-04 15:23:55 +1000 (Wed, 04 Sep 2013) $
-- $LastChangedRevision: 12590 $

.LABEL EXITERR
.QUIT 4
