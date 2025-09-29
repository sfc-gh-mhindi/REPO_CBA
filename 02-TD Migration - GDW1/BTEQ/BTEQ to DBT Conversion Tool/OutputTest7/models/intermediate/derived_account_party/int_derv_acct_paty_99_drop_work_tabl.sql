-- =====================================================================
-- DBT Model: DERV_ACCT_PATY_99_DROP_WORK_TABL
-- Converted from BTEQ: DERV_ACCT_PATY_99_DROP_WORK_TABL.sql
-- Category: derived_account_party
-- Original Size: 5.3KB, 226 lines
-- Complexity Score: 251
-- Generated: 2025-08-21 13:48:49
-- =====================================================================

{{ intermediate_model_config() }}




------------------------------------------------------------------------------
-- Object Name             :  DERV_ACCT_PATY_99_DROP_WORK_TABL.sql
-- Object Type             :  BTEQ

--                           
-- Description             :  Drop work/staging tables that were used in this stream  
--                          
------------------------------------------------------------------------------
-- Modification History
-- Date               Author           Version     Version Description
-- 04/06/2013         Helen Zak        1.0         Initial Version
-- 08/08/2013         Helen Zak        1.1         C0714578  - post-implementation fixes
--                                                 drop new staging tables 
-- 21/08/2013         Helen Zak        1.2         C0726912 -post-implementation fix
--                                                 Remove redundant tables 
-- 02/09/2013         Helen Zak        1.3         C0737261-post-implementation fix
--                                                 Drop new staging tables 
-- 11/09/2013         Helen Zak        1.4         C0746488-post-implementation fix
--                                                 Drop new staging tables 
-- 27/03/2025		  Sourabh Kath	   1.5		   Drop 2 new staging tables created for
--												   Preferred Party Changes
------------------------------------------------------------------------------

-- Drop all work tables if we are in PROD

SELECT 1 FROM 
(SELECT CASE WHEN '{{ bteq_var("ENV_C") }}' = 'PROD' THEN 1 ELSE 0 END AS ENV) T1 
WHERE T1.ENV = 1;


-- if no row returned - we are in DEV. Do not drop the tables. 
-- Complete the process successfully.


DROP TABLE {{ bteq_var("DDSTG") }}.ACCT_PATY_REL_THA;



DROP TABLE {{ bteq_var("DDSTG") }}.ACCT_PATY_THA_NEW_RNGE;





DROP TABLE {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_CURR;



DROP TABLE {{ bteq_var("DDSTG") }}.DERV_PRTF_ACCT_STAG;


DROP TABLE {{ bteq_var("DDSTG") }}.DERV_PRTF_PATY_STAG;


DROP TABLE {{ bteq_var("DDSTG") }}.DERV_PRTF_ACCT_PATY_STAG;



DROP  TABLE {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_RM;




DROP TABLE {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_ADD;


DROP TABLE {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_CHG;


DROP TABLE {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_FLAG;



DROP TABLE {{ bteq_var("DDSTG") }}.ACCT_PATY_REL_WSS;



DROP TABLE {{ bteq_var("DDSTG") }}.ACCT_REL_WSS_DITPS;





DROP TABLE {{ bteq_var("DDSTG") }}.GRD_GNRC_MAP_DERV_PATY_HOLD;


DROP TABLE {{ bteq_var("DDSTG") }}.GRD_GNRC_MAP_DERV_UNID_PATY;


DROP TABLE {{ bteq_var("DDSTG") }}.GRD_GNRC_MAP_DERV_PATY_REL;


DROP TABLE {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_DEL;



DROP TABLE {{ bteq_var("DDSTG") }}.ACCT_PATY_DEDUP;



DROP TABLE {{ bteq_var("DDSTG") }}.DERV_PRTF_ACCT_PATY_PSST;



DROP  TABLE {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_NON_RM;


DROP  TABLE {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_ROW_SECU_FIX;



DROP  TABLE {{ bteq_var("DDSTG") }}.GRD_GNRC_MAP_PATY_HOLD_PRTY;



DROP  TABLE {{ bteq_var("DDSTG") }}.GRD_GNRC_MAP_BUSN_SEGM_PRTY;



-- $LastChangedBy: zakhe $
-- $LastChangedDate: 2013-09-11 15:36:32 +1000 (Wed, 11 Sep 2013) $
-- $LastChangedRevision: 12624 $

