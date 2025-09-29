-- =====================================================================
-- DBT Model: DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG
-- Converted from BTEQ: DERV_ACCT_PATY_06_SET_MAX_PRFR_FLAG.sql
-- Category: derived_account_party
-- Original Size: 12.7KB, 333 lines
-- Complexity Score: 128
-- Generated: 2025-08-21 14:21:55
-- =====================================================================

{{ intermediate_model_config() }}




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
-- 11/03/2025		 Sourabh Kath		1.4			Made Changes for Preferred Party
------------------------------------------------------------------------------
   -- Identify non-RM managed accounts and insert them into a separate table
 
   
-- Original -- Original DELETE removed: DELETE removed: DELETE FROM {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_NON_RM ALL;
-- DBT handles table replacement via materialization strategy
-- DBT handles table replacement via materialization strategy

USING 
( EXTR_D VARCHAR(10) )  
 -- Original INSERT converted to SELECT for DBT intermediate model
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

FROM {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_CURR DAP
LEFT JOIN {{ bteq_var("DDSTG") }}.DERV_PRTF_ACCT_PATY_STAG T1

  ON DAP.ASSC_ACCT_I = T1.ACCT_I
WHERE :EXTR_D BETWEEN DAP.EFFT_D AND DAP.EXPY_D
AND T1.ACCT_I IS NULL
GROUP BY 1,2,3,4,5,6,7,8,9,10;

      
   COLLECT STATS    {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_NON_RM;
 
 
 
 
 
/* 
	BEGIN  C2039849_IM1317265_Revenue_Attribution_to_Fund_Holder for Non RM Accounts 
	1. The Below Logic gets the priority to be set for all 
	   the Customers having Multiple Fund holders relationship 
	2. This uses ODS Table Called ODS_RULE to understand the Priority List 	
	
*/
USING 
( EXTR_D VARCHAR(10) )  
UPDATE  DAPNR
  FROM  {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_NON_RM  DAPNR,
  	(SELECT DISTINCT  PIG.PATY_I			  			  
    	 	          ,GDFVC.PRTY
          FROM  PVDATA.PATY_INT_GRUP_CURR PIG 				
    INNER JOIN  PVDATA.INT_GRUP_DEPT_CURR IGD 				
      	     ON  IGD.INT_GRUP_I=PIG.INT_GRUP_I			 						   
           AND  PIG.REL_C = 'RLMT'
    INNER JOIN (   	SELECT  GFC.DEPT_LEAF_NODE_C AS DEPT_I
       			,ORU.PRTY PRTY 
		 	 FROM   {{ bteq_var("VTECH") }}.GRD_DEPT_FLAT_CURR  GFC 
		   LEFT JOIN  (
				   SELECT  COALESCE(PRTY,9999) PRTY 
	      				   ,LKUP1_TEXT
		      			   ,COALESCE(UPDT_DTTS,CRAT_DTTS) LoadTimeStamp
   		  		     FROM  {{ bteq_var("VTECH") }}.ODS_RULE --{{ bteq_var("VTECH") }} Should be replaced by 
 				    WHERE  RULE_CODE = 'RMPOC'			
		  		      AND :EXTR_D BETWEEN VALD_FROM AND VALD_TO
	       		  QUALIFY  ROW_NUMBER() OVER (PARTITION BY LKUP1_TEXT,PRTY ORDER BY LoadTimeStamp DESC) = 1		
		     		 )ORU
		          ON   GFC.DEPT_L3_NODE_C = ORU.LKUP1_TEXT
		       WHERE   PRTY <> 9999 ) GDFVC
	     ON   GDFVC.DEPT_I = IGD.DEPT_I
 	  WHERE   PIG.PATY_I IN (  SELECT  DISTINCT PATY_I 
	      			    	 FROM  {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_CURR)		     			    	 	 		 	
	QUALIFY   ROW_NUMBER() OVER (PARTITION BY PIG.PATY_I ORDER BY GDFVC.PRTY ASC) = 1)DRVD
   SET  RANK_I = DRVD.PRTY
 WHERE  DAPNR.PATY_I = DRVD.PATY_I
   AND  DAPNR.PATY_ACCT_REL_C= 'ZINTE0';

UPDATE  {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_NON_RM
   SET  RANK_I = CASE WHEN RANK_I <> 99 AND PATY_ACCT_REL_C = 'ZINTE0' 	   /* Any Fund Holder relationship with With Account would be a part of Rule Priority Wil get First Priroty */   	           	 
   		        THEN RANK_I 		      
	 	        WHEN PATY_ACCT_REL_C = 'ZINTE0'          		   /* Any Fund Holder Part of FundHolder Would Get Second Priority */
	 	        THEN 98 	      
			 ELSE 99            				      		   /* Any Remaining Would Get Least Prirotty */
      		    END;

/* END   C2039849 _IM1317265_ Revenue_Attribution_to_Fund_Holder Change for NON RM Accounts   */			 


 
-- Determine MAX(PATY_I) for each account and, as all rows are current, remove
-- those that have EFFT_D less than the effective date of the row with MAX(PATY_I)

USING 
( EXTR_D VARCHAR(10) )   
UPDATE DAP
  FROM {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_NON_RM DAP,
      (SELECT T1.ACCT_I      
	      ,T1.PATY_I
	      ,T1.PATY_aCCT_REL_C
	      ,T1.EFFT_D
	      ,T1.EXPY_D
        FROM  {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_NON_RM T1
        JOIN 	{{ bteq_var("DDSTG") }}.GRD_GNRC_MAP_DERV_PATY_HOLD T2  
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

 


-- Insert rows for the accounts for which the flag was set (both RM and non-RM).
-- Use fast path for efficiency.


DEL FROM {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_FLAG ALL;
 


-- Original INSERT converted to SELECT for DBT intermediate model
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

FROM {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_RM
GROUP BY 1,2,3,4,5,6,7,8,9;-- Original INSERT converted to SELECT for DBT intermediate model
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

FROM {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_NON_RM
GROUP BY 1,2,3,4,5,6,7,8,9;


COLLECT STATS {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_FLAG;



--Preferred Party Changes Start here
--Business Rules are mentioned here: https://commbank.atlassian.net/wiki/spaces/BPR/pages/970007565/GARE+Customer+Segmentation+Rules
--Solution Outline is mentioned here: https://commbank.atlassian.net/wiki/spaces/BPR/pages/1068698006/Acct+Paty+Fixes+-+Preferred+Paty

--Overriding Preferred Party Flag to 'N' for all the accounts which are:
--1. Currently active
--2. Have more than 1 Distinct Party
--3. Have atleast one of the role from the selected list

USING 
( EXTR_D VARCHAR(10) )   
UPDATE {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_FLAG
SET PRFR_PATY_F = 'N'
WHERE ACCT_I IN
(SELECT DISTINCT DAP.ACCT_I
FROM {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_FLAG DAP
INNER JOIN {{ bteq_var("VTECH") }}.GRD_GNRC_MAP_CURR DPH
ON DPH.MAP_TYPE_C	= 'DERV_ACCT_PATY_HOLD_REL_MAP'
AND DPH.SRCE_CHAR_1_C = DAP.PATY_ACCT_REL_C
GROUP BY 1 HAVING Count(DISTINCT DAP.PATY_I) > 1
)
;



USING 
( EXTR_D VARCHAR(10) )   
UPDATE DAP1
FROM {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_FLAG DAP1,
(
 SELECT
  DAP.ACCT_I
 ,DAP.PATY_I
 ,DAP.PATY_ACCT_REL_C
 ,DAP.EXPY_D
 FROM {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_FLAG DAP

 --Below table contains the list of Roles which needs to be considered along with the Priority.
 --Priority is required because an Account can have multiple parties and each Party can belong to multiple roles.
 --Eventually for a given account, we need just 1 Party from only 1 Relationship code to be flagged as Preferred Party Flag = 'Y'
 INNER JOIN {{ bteq_var("DDSTG") }}.GRD_GNRC_MAP_PATY_HOLD_PRTY DPH
 ON  DPH.MAP_TYPE_C	= 'DERV_ACCT_PATY_HOLD_REL_MAP'
 AND DPH.PATY_ACCT_REL_C = DAP.PATY_ACCT_REL_C
 
 --To get the Portfolio to which the Party belong
 LEFT JOIN {{ bteq_var("VTECH") }}.PATY_INT_GRUP PI1
 ON  PI1.PATY_I	= DAP.PATY_I
 AND PI1.REL_C	= 'RLMT'
 AND :EXTR_D BETWEEN PI1.EFFT_D AND PI1.EXPY_D
 AND :EXTR_D BETWEEN PI1.VALD_FROM_D AND PI1.VALD_TO_D
 
 --To get the Parent Department to which the Portfolio belong
 LEFT JOIN {{ bteq_var("VTECH") }}.INT_GRUP_DEPT IGD
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
  FROM {{ bteq_var("VTECH") }}.DEPT_DIMN_NODE_ANCS_CURR DA1
  INNER JOIN {{ bteq_var("DDSTG") }}.GRD_GNRC_MAP_BUSN_SEGM_PRTY BSP1 --Priority of different Departments defined here
  ON DA1.ANCS_DEPT_I = BSP1.BUSN_SEGM_C
  AND BSP1.MAP_TYPE_C = 'BUSN_SEGM_PRTY'
 ) SEGM_PRTY
 ON SEGM_PRTY.DEPT_I = IGD.DEPT_I

 --To check if the Portfolio is Banker Managed or Department managed
 LEFT JOIN {{ bteq_var("VTECH") }}.DERV_PRTF_OWN_REL DPOR
 ON  DPOR.PRTF_CODE_X = Right(PI1.INT_GRUP_I,7)
 AND DPOR.ROLE_PLAY_TYPE_X = 'Employee'
 AND DPOR.DERV_PRTF_TYPE_C = 'RLMT'
 AND DPOR.PRTF_CODE_X <> 'NA'
 AND :EXTR_D BETWEEN DPOR.VALD_FROM_D AND DPOR.VALD_TO_D
 AND :EXTR_D BETWEEN DPOR.EFFT_D AND DPOR.EXPY_D

 --Updating only those accounts which have more than 1 unique Party Id
 WHERE DAP.ACCT_I IN (SELECT ACCT_I FROM {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_FLAG GROUP BY 1 HAVING Count(DISTINCT PATY_I) > 1)

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


COLLECT STATS {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_FLAG;



-- $LastChangedBy: zakhe $
-- $LastChangedDate: 2013-09-04 15:23:55 +1000 (Wed, 04 Sep 2013) $
-- $LastChangedRevision: 12590 $

