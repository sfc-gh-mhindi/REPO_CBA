-- =====================================================================
-- DBT Model: ACCT_BALN_BKDT_RECN_ISRT
-- Converted from BTEQ: ACCT_BALN_BKDT_RECN_ISRT.sql
-- Category: account_balance
-- Original Size: 5.0KB, 238 lines
-- Complexity Score: 80
-- Generated: 2025-08-21 15:46:51
-- =====================================================================

{{ intermediate_model_config() }}


----------------------------------------------------------------------
-- $LastChangedBy: vajapes $
-- $LastChangedDate: 2012-05-03 14:09:15 +1000 (Thu, 03 May 2012) $
-- $LastChangedRevision: 9596 $
----------------------------------------------------------------------
------------------------------------------------------------------------------
--
--  Description :Reconciliation process
--
--   Ver  Date       Modified By            Description
--  ---- ---------- ---------------------- -----------------------------------
--  1.0  2011-10-05 Suresh Vajapeyajula               Initial Version
------------------------------------------------------------------------------

/*-- Original DELETE removed: Delete previous run data from ACCT_BALN_BKDT_RECN*/
DELETE {{ bteq_var("CAD_PROD_DATA") }}.ACCT_BALN_BKDT_RECN;
-- DBT handles table replacement via materialization strategy


/*Insert data into ACCT_BALN_BKDT_RECN table*/
-- Original INSERT converted to SELECT for DBT intermediate model
SELECT 
DT.ACCT_I
,BAL.EFFT_D
,BAL.EXPY_D
,DT.BALN_A
,NULL AS PROS_KEY_EFFT_I
FROM
(
SELECT 
B.ACCT_I,
B.BALN_A
FROM
/*Qualifying only those records that are considered for applying adjustments as part of this run*/
{{ bteq_var("DDSTG") }}.ACCT_BALN_BKDT_STG2 A
INNER JOIN
{{ bteq_var("VTECH") }}.ACCT_BALN B
ON
A.ACCT_I=B.ACCT_I
WHERE
B.BALN_TYPE_C='BALN'
AND B.CALC_FUNC_C='SPOT' 
AND B.TIME_PERD_C = 'E' 
/*reconciling the balances only for the current record*/
AND CURRENT_DATE BETWEEN B.EFFT_D AND B.EXPY_D

MINUS

SELECT 
STG.ACCT_I,
(CASE WHEN STG.BKDT_EFFT_D > BKDT.BKDT_EFFT_D  THEN STG.BALN_A 
ELSE BKDT.BALN_A END ) AS BALN_A
FROM
(
SELECT 
A.ACCT_I
,A.BKDT_EFFT_D
,A.BKDT_EXPY_D
,A.BALN_A
FROM
{{ bteq_var("DDSTG") }}.ACCT_BALN_BKDT_STG2 A
WHERE
A.BALN_TYPE_C='BDCL'
AND A.CALC_FUNC_C='SPOT' 
AND A.TIME_PERD_C = 'E' 
AND CURRENT_DATE BETWEEN A.BKDT_EFFT_D AND A.BKDT_EXPY_D
)STG
INNER JOIN
(
SELECT 
B.ACCT_I
,B.BKDT_EFFT_D
,B.BKDT_EXPY_D
,B.BALN_A
FROM
{{ bteq_var("DDSTG") }}.ACCT_BALN_BKDT_STG2 A
INNER JOIN
{{ bteq_var("VTECH") }}.ACCT_BALN_BKDT B
ON
A.ACCT_I = B.ACCT_I
WHERE
B.BALN_TYPE_C='BDCL'
AND B.CALC_FUNC_C='SPOT' 
AND B.TIME_PERD_C = 'E' 
AND CURRENT_DATE BETWEEN B.BKDT_EFFT_D AND B.BKDT_EXPY_D
)BKDT
ON
STG.ACCT_I = BKDT.ACCT_I
)DT
INNER JOIN
{{ bteq_var("VTECH") }}.ACCT_BALN BAL
ON
DT.ACCT_I = BAL.ACCT_I
WHERE
BAL.BALN_TYPE_C='BDCL'
AND BAL.CALC_FUNC_C='SPOT' 
AND BAL.TIME_PERD_C = 'E' 
AND CURRENT_DATE BETWEEN BAL.EFFT_D AND BAL.EXPY_D;





-- Original INSERT converted to SELECT for DBT intermediate model
SELECT 
 DT1.ACCT_I
,STG.BKDT_EFFT_D
,STG.BKDT_EXPY_D
,STG.BALN_A
,NULL AS PROS_KEY_EFFT_I
FROM
(SELECT 
STG.ACCT_I,
(CASE WHEN STG.BKDT_EFFT_D > BKDT.BKDT_EFFT_D  THEN STG.BALN_A 
ELSE BKDT.BALN_A END ) AS BALN_A,
NULL AS PROS_KEY_EFFT_I
FROM
(
SELECT 
A.ACCT_I
,A.BKDT_EFFT_D
,A.BKDT_EXPY_D
,A.BALN_A
FROM
{{ bteq_var("DDSTG") }}.ACCT_BALN_BKDT_STG2 A
WHERE
A.BALN_TYPE_C='BDCL'
AND A.CALC_FUNC_C='SPOT' 
AND A.TIME_PERD_C = 'E' 
AND CURRENT_DATE BETWEEN A.BKDT_EFFT_D AND A.BKDT_EXPY_D
)STG
INNER JOIN
(
SELECT 
B.ACCT_I
,B.BKDT_EFFT_D
,B.BKDT_EXPY_D
,B.BALN_A
FROM
{{ bteq_var("DDSTG") }}.ACCT_BALN_BKDT_STG2 A
INNER JOIN
{{ bteq_var("VTECH") }}.ACCT_BALN_BKDT B
ON
A.ACCT_I = B.ACCT_I
WHERE
B.BALN_TYPE_C='BDCL'
AND B.CALC_FUNC_C='SPOT' 
AND B.TIME_PERD_C = 'E' 
AND CURRENT_DATE BETWEEN B.BKDT_EFFT_D AND B.BKDT_EXPY_D
)BKDT
ON
STG.ACCT_I = BKDT.ACCT_I

MINUS

SELECT 
B.ACCT_I,
B.BALN_A,
NULL AS PROS_KEY_EFFT_I
FROM
/*Qualifying only those records that are considered for applying adjustments as part of this run*/
{{ bteq_var("DDSTG") }}.ACCT_BALN_BKDT_STG2 A
INNER JOIN
{{ bteq_var("VTECH") }}.ACCT_BALN B
ON
A.ACCT_I=B.ACCT_I
WHERE
B.BALN_TYPE_C='BALN'
AND B.CALC_FUNC_C='SPOT' 
AND B.TIME_PERD_C = 'E' 
/*reconciling the balances only for the current record*/
AND CURRENT_DATE BETWEEN B.EFFT_D AND B.EXPY_D
)DT1
INNER JOIN
{{ bteq_var("DDSTG") }}.ACCT_BALN_BKDT_STG2 STG
ON
DT1.ACCT_I=STG.ACCT_I
AND CURRENT_DATE BETWEEN STG.BKDT_EFFT_D AND STG.BKDT_EXPY_D;


/*Activity Count > 0 implies - An adjustment affecting the balances of an open record for 
the ACCT_I loaded into ACCT_BALN_BKDT_RECN table.This impacts the next daily delta 
load of Datastage. Thereby it is highly recommended to fix the issue 
before you restart in such a failures*/



/*Update the flags,counts for the process in UTIl_PROS_ISAC*/
UPDATE {{ bteq_var("CAD_PROD_DATA") }}.UTIL_PROS_ISAC
FROM
(SELECT COUNT(*) FROM 
{{ bteq_var("VTECH") }}.ACCT_BALN_BKDT_RECN)A(INS_CNT)
SET     COMT_F = 'Y',
	SUCC_F='Y',
	COMT_S =  CURRENT_TIMESTAMP(0),
	SYST_INS_Q = A.INS_CNT
 WHERE PROS_KEY_I = (SELECT MAX(PROS_KEY_I) FROM {{ bteq_var("VTECH") }}.UTIL_PROS_ISAC 
 WHERE CONV_M='CAD_X01_ACCT_BALN_BKDT_RECN') ;






/*Updating the records with the latest pros key */
UPDATE {{ bteq_var("CAD_PROD_DATA") }}.ACCT_BALN_BKDT_RECN
FROM
(SELECT MAX(PROS_KEY_I) AS PROS_KEY_I
FROM {{ bteq_var("VTECH") }}.UTIL_PROS_ISAC 
WHERE CONV_M= 'CAD_X01_ACCT_BALN_BKDT_RECN') D (PROS_KEY_I)
SET PROS_KEY_EFFT_I = D.PROS_KEY_I;


