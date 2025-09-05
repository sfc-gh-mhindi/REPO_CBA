.RUN FILE=%%BTEQ_LOGON_SCRIPT%%

.IF ERRORLEVEL <> 0 THEN .GOTO EXITERR

.SET QUIET OFF
.SET ECHOREQ ON
.SET FORMAT OFF
.SET WIDTH 120

------------------------------------------------------------------------------
-- Object Name             :  DERV_ACCT_PATY_03_SET_ACCT_PRTF.sql
-- Object Type             :  BTEQ

--                           
-- Description             :  Get accounts that are relationship managed and ONLY ONE
--                            of the parties oin this account is relationship managed 
--                            by the same RM. This party will be a preferred party for
--                            such an account.   
--                          
------------------------------------------------------------------------------
-- Modification History
-- Date               Author           Version     Version Description
-- 04/06/2013         Helen Zak        1.0         Initial Version
--07/08/2013          Helen Zak        1.1         C0714578 - post-implementation fix
--                                                 Use persisted GRD table for better performance 
--                                                 Only get existing rows that have the flag set to 'N'
--                                                 as otherwise they don't need to change 
 -- 21/08/2013       Helen Zak       1.2          C0726912  - post-implementation fix
 --                                                                         Remove logic of getting existing rows
 --                                                                         as it should be handled by the delta process (in theory)
------------------------------------------------------------------------------

-- Get account portfolio details as per the extract date


DELETE FROM K_PDDSTG.DERV_PRTF_ACCT_STAG;
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR 
.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
USING 
( EXTR_D VARCHAR(10) )
INSERT INTO K_PDDSTG.DERV_PRTF_ACCT_STAG
 SELECT ACCT_I
        ,PRTF_CODE_X
   FROM  K_PVTECH.DERV_PRTF_ACCT 
 
 WHERE PERD_D = '2025-07-10'
   AND DERV_PRTF_CATG_C = 'RM'
    
 GROUP BY 1,2;
 
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR 

COLLECT STATS K_PDDSTG.DERV_PRTF_ACCT_STAG;
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR 

-- Get party portfolio details as per the extract date

DELETE FROM K_PDDSTG.DERV_PRTF_PATY_STAG;
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR 

.IMPORT VARTEXT FILE=/cba_app/CBMGDW/%%ENV_C%%/schedule/%%STRM_C%%_extr_date.txt
USING 
( EXTR_D VARCHAR(10) )
INSERT INTO K_PDDSTG.DERV_PRTF_PATY_STAG
 SELECT PATY_I
        ,PRTF_CODE_X
   FROM  K_PVTECH.DERV_PRTF_PATY 
 
 WHERE PERD_D = '2025-07-10'
   AND DERV_PRTF_CATG_C = 'RM'
 
 GROUP BY 1,2;
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR 

COLLECT STATS K_PDDSTG.DERV_PRTF_PATY_STAG;
.IF ERRORCODE   <> 0 THEN .GOTO EXITERR 


 
.QUIT 0

-- $LastChangedBy: zakhe $
-- $LastChangedDate: 2013-08-22 12:10:19 +1000 (Thu, 22 Aug 2013) $
-- $LastChangedRevision: 12529 $

.LABEL EXITERR
.QUIT 4
