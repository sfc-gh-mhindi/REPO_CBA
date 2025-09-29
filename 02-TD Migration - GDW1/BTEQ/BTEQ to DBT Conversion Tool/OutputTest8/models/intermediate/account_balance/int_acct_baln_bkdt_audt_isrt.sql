-- =====================================================================
-- DBT Model: ACCT_BALN_BKDT_AUDT_ISRT
-- Converted from BTEQ: ACCT_BALN_BKDT_AUDT_ISRT.sql
-- Category: account_balance
-- Original Size: 3.7KB, 114 lines
-- Complexity Score: 42
-- Generated: 2025-08-21 13:57:48
-- =====================================================================

{{ intermediate_model_config() }}


----------------------------------------------------------------------
-- $LastChangedBy: vajapes $
-- $LastChangedDate: 2012-02-28 09:08:37 +1100 (Tue, 28 Feb 2012) $
-- $LastChangedRevision: 9218 $
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

/*Loading the records from ACCT_BALN_BKDT_STG1 to ACCT_BALN_BKDT_AUDT.
This table holds data that is going to be deleted from ACCT_BALN_BKDT. 
The pros keys in this table can be used to establish logical relationship between tables in the event 
of any failures and rollback. However, the Pros Keys from ACCT_BALN_ADJ  loaded as 
ADJ_PROS_KEY_EFFT_I could not be used to backtrack Adjustments for bulk loads or for 
multiple adjustments for same ACCT_I and overlapping preiods. A known limitation
*/
-- Original INSERT converted to SELECT for DBT intermediate model
SELECT 
STG1.ACCT_I,                        
STG1.BALN_TYPE_C,
STG1.CALC_FUNC_C,                  
STG1.TIME_PERD_C,                  
STG1.BALN_A,
STG1.CALC_F,
STG1.SRCE_SYST_C,
STG1.ORIG_SRCE_SYST_C,
STG1.LOAD_D,
STG1.BKDT_EFFT_D,
STG1.BKDT_EXPY_D,
PKEY.PROS_KEY_EFFT_I,
STG1.PROS_KEY_EFFT_I AS ABAL_PROS_KEY_EFFT_I,
STG1.PROS_KEY_EXPY_I AS ABAL_PROS_KEY_EXPY_I,
STG2.BKDT_PROS_KEY_I AS ABAL_BKDT_PROS_KEY_I,
ADJ.PROS_KEY_EFFT_I AS ADJ_PROS_KEY_EFFT_I
FROM 
{{ bteq_var("DDSTG") }}.ACCT_BALN_BKDT_STG1 STG1
INNER JOIN
/*Capturing the maximum pros_key_efft_i in the event of multiple pros keys for one account 
and populating for Auditing purposes*/
(SELECT ACCT_I, MAX(PROS_KEY_EFFT_I) AS PROS_KEY_EFFT_I FROM
{{ bteq_var("DDSTG") }}.ACCT_BALN_BKDT_ADJ_RULE
GROUP BY 1
)ADJ
ON
STG1.ACCT_I = ADJ.ACCT_I
CROSS JOIN
/*Capture tha latest pros key [from the parent process] and update the audt table*/
(SELECT MAX(BKDT_PROS_KEY_I) AS BKDT_PROS_KEY_I
FROM {{ bteq_var("DDSTG") }}.ACCT_BALN_BKDT_STG2)STG2
CROSS JOIN
/*Capture tha latest pros key [from the Auditing process] and update the audt table*/
(SELECT MAX(PROS_KEY_I)  AS PROS_KEY_EFFT_I
FROM {{ bteq_var("VTECH") }}.UTIL_PROS_ISAC WHERE 
CONV_M='CAD_X01_ACCT_BALN_BKDT_AUDT')PKEY;



/*Update UTIL_PROS_ISAC with the  flags , completed timestamp and record counts inserted*/
UPDATE {{ bteq_var("CAD_PROD_DATA") }}.UTIL_PROS_ISAC
FROM
(SELECT COUNT(*) FROM 
{{ bteq_var("DDSTG") }}.ACCT_BALN_BKDT_STG1)A(INS_CNT)
SET  
        COMT_F = 'Y',
	SUCC_F='Y',
	COMT_S =  CURRENT_TIMESTAMP(0),
	SYST_INS_Q = A.INS_CNT
 WHERE PROS_KEY_I = (SELECT MAX(PROS_KEY_I) FROM {{ bteq_var("VTECH") }}.UTIL_PROS_ISAC 
 WHERE CONV_M='CAD_X01_ACCT_BALN_BKDT_AUDT');



