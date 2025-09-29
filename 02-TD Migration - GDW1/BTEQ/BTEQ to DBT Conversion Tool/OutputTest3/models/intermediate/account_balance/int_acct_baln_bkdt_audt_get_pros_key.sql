-- =====================================================================
-- DBT Model: ACCT_BALN_BKDT_AUDT_GET_PROS_KEY
-- Converted from BTEQ: ACCT_BALN_BKDT_AUDT_GET_PROS_KEY.sql
-- Category: account_balance
-- Original Size: 5.4KB, 177 lines
-- Complexity Score: 34
-- Generated: 2025-08-21 10:51:05
-- =====================================================================

{{ intermediate_model_config() }}


----------------------------------------------------------------------
-- $LastChangedBy: vajapes $
-- $LastChangedDate: 2012-02-28 09:08:27 +1100 (Tue, 28 Feb 2012) $
-- $LastChangedRevision: 9217 $
----------------------------------------------------------------------
------------------------------------------------------------------------------
--
--  Description :Populate the AUDT table for future reference as the records 
--are going to be deleted from ACCT BALN BKDT and this acts as a driver for the 
--ADJ RULE view 
--
--   Ver  Date       Modified By            Description
--  ---- ---------- ---------------------- -----------------------------------
--  1.0  2011-10-05 Suresh Vajapeyajula               Initial Version
------------------------------------------------------------------------------


LOCKING TABLE {{ bteq_var("CAD_PROD_DATA") }}.UTIL_PARM FOR WRITE
/*Capture the Latest Pros Key from UTIL PARM table and update UTIL PROS ISAC.
The standard stored procedure SP_GET_PROS_KEY is not used in this solution as this is not a batch load. 
And this approach was discussed and agreed with BI Services. */
INSERT INTO {{ bteq_var("CAD_PROD_DATA") }}.UTIL_PROS_ISAC
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
'CAD_X01_ACCT_BALN_BKDT_AUDT'  AS  CONV_M , 
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
/*Applying the adjustments from ACCT_BALN on ACCT_BALN_BKDT _AUDT*/
'ACCT_BALN_BKDT' AS  SRCE_M ,
'ACCT_BALN_BKDT_AUDT' AS  TRGT_M ,
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
{{ bteq_var("VTECH") }}.UTIL_PARM PARM
CROSS JOIN 
/*Inserts multiple records for each batch run in the event of any delays or failures. 
Capture latest Batch run date relating to the Latest delta load into ACCT_BALN_BKDT*/
(SELECT  
CAL.CALR_CALR_D
FROM
/*Capture last Succesful Batch run date relating to the Backdated adjustment solution */
(SELECT  MAX(BTCH_RUN_D) AS BTCH_RUN_D 
FROM  {{ bteq_var("VTECH") }}.UTIL_PROS_ISAC
WHERE    
TRGT_M='ACCT_BALN_BKDT' 
AND SRCE_SYST_M='GDW'
AND COMT_F = 'Y'  
AND SUCC_F='Y') BKDT_PREV
CROSS JOIN 
/*Capture latest Batch run date relating to the Backdated adjustment solution into ACCT_BALN_BKDT*/
(SELECT  MAX(BTCH_RUN_D) AS BTCH_RUN_D 
FROM  {{ bteq_var("VTECH") }}.UTIL_PROS_ISAC
WHERE    
TRGT_M='ACCT_BALN_BKDT' 
AND SRCE_SYST_M='GDW') BKDT_CURR

,{{ bteq_var("VTECH") }}.GRD_RPRT_CALR_CLYR CAL

WHERE CAL.CALR_CALR_D > BKDT_PREV.BTCH_RUN_D 
AND CAL.CALR_CALR_D  <= BKDT_CURR.BTCH_RUN_D
)DT
WHERE 
PARM.PARM_M='PROS_KEY'

 
/*Increment PROS KEY by 1 and update UTIL PARM table*/
;UPDATE {{ bteq_var("CAD_PROD_DATA") }}.UTIL_PARM
SET PARM_LTRL_N = PARM_LTRL_N + 1
WHERE
PARM_M='PROS_KEY';


.QUIT 0
.LOGOFF
.EXIT

.LABEL EXITERR
.QUIT 1
.LOGOFF
.EXIT