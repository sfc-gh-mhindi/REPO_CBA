-- =====================================================================
-- DBT Model: DERV_ACCT_PATY_99_DROP_WORK_TABL_CHG0379808
-- Converted from BTEQ: DERV_ACCT_PATY_99_DROP_WORK_TABL_CHG0379808.sql
-- Category: derived_account_party
-- Original Size: 4.9KB, 208 lines
-- Complexity Score: 230
-- Generated: 2025-08-21 10:51:05
-- =====================================================================

{{ intermediate_model_config() }}


.IF ERRORLEVEL <> 0 THEN 

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
------------------------------------------------------------------------------

-- Drop all work tables if we are in PROD

SELECT 1 FROM 
(SELECT CASE WHEN '{{ bteq_var("ENV_C") }}' = 'PROD' THEN 1 ELSE 0 END AS ENV) T1 
WHERE T1.ENV = 1;


-- if no row returned - we are in DEV. Do not drop the tables. 
-- Complete the process successfully.

.IF ACTIVITYCOUNT = 0 THEN 
DROP TABLE {{ bteq_var("DDSTG") }}.ACCT_PATY_REL_THA;

.QUIT ERRORCODE

.LABEL NEXT1

DROP TABLE {{ bteq_var("DDSTG") }}.ACCT_PATY_THA_NEW_RNGE;

.QUIT ERRORCODE

.LABEL NEXT2



DROP TABLE {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_CURR;

.QUIT ERRORCODE

.LABEL NEXT3

DROP TABLE {{ bteq_var("DDSTG") }}.DERV_PRTF_ACCT_STAG;
.QUIT ERRORCODE

.LABEL NEXT4

DROP TABLE {{ bteq_var("DDSTG") }}.DERV_PRTF_PATY_STAG;
.QUIT ERRORCODE

.LABEL NEXT5

DROP TABLE {{ bteq_var("DDSTG") }}.DERV_PRTF_ACCT_PATY_STAG;
.QUIT ERRORCODE

.LABEL NEXT6


DROP  TABLE {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_RM;
.QUIT ERRORCODE

.LABEL NEXT7



DROP TABLE {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_ADD;
.QUIT ERRORCODE

.LABEL NEXT8

DROP TABLE {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_CHG;
.QUIT ERRORCODE

.LABEL NEXT9

DROP TABLE {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_FLAG;
.QUIT ERRORCODE

.LABEL NEXT10


DROP TABLE {{ bteq_var("DDSTG") }}.ACCT_PATY_REL_WSS;
.QUIT ERRORCODE

.LABEL NEXT11


DROP TABLE {{ bteq_var("DDSTG") }}.ACCT_REL_WSS_DITPS;
.QUIT ERRORCODE

.LABEL NEXT12




DROP TABLE {{ bteq_var("DDSTG") }}.GRD_GNRC_MAP_DERV_PATY_HOLD;
.QUIT ERRORCODE

.LABEL NEXT13

DROP TABLE {{ bteq_var("DDSTG") }}.GRD_GNRC_MAP_DERV_UNID_PATY;
.QUIT ERRORCODE

.LABEL NEXT14

DROP TABLE {{ bteq_var("DDSTG") }}.GRD_GNRC_MAP_DERV_PATY_REL;
.QUIT ERRORCODE

.LABEL NEXT15

DROP TABLE {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_DEL;
.QUIT ERRORCODE

.LABEL NEXT16


DROP TABLE {{ bteq_var("DDSTG") }}.ACCT_PATY_DEDUP;

.QUIT ERRORCODE

.LABEL NEXT17

DROP TABLE {{ bteq_var("DDSTG") }}.DERV_PRTF_ACCT_PATY_PSST;

.QUIT ERRORCODE

.LABEL NEXT18

DROP  TABLE {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_NON_RM;
.QUIT ERRORCODE

.LABEL NEXT19

DROP  TABLE {{ bteq_var("DDSTG") }}.DERV_ACCT_PATY_ROW_SECU_FIX;
.QUIT ERRORCODE

.LABEL NEXT20

.LABEL EXITOK
.QUIT 0

-- $LastChangedBy: zakhe $
-- $LastChangedDate: 2013-09-11 15:36:32 +1000 (Wed, 11 Sep 2013) $
-- $LastChangedRevision: 12624 $

.LABEL EXITERR
.QUIT 4
