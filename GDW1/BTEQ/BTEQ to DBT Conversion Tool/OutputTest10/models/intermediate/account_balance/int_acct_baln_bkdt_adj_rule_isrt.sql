-- =====================================================================
-- DBT Model: ACCT_BALN_BKDT_ADJ_RULE_ISRT
-- Converted from BTEQ: ACCT_BALN_BKDT_ADJ_RULE_ISRT.sql
-- Category: account_balance
-- Original Size: 4.9KB, 164 lines
-- Complexity Score: 38
-- Generated: 2025-08-21 15:46:51
-- =====================================================================

{{ intermediate_model_config() }}



------------------------------------------------------------------------------
--  SCRIPT NAME: 90_ISRT_ACCT_BALN_BKDT_ADJ_RULE
--Description: Calculate the Backdated adjustment from ACCT BALN ADJ and apply 
--it on ACCT BALN
--
--  Ver  Date       Modified By        		    Description
--  ---- ---------- ---------------------------------------------------------
--  1.0  22/07/2011 Suresh Vajapeyajula        	Initial Version

-- $LastChangedBy: vajapes $
-- $LastChangedDate: 2012-02-28 09:08:17 +1100 (Tue, 28 Feb 2012) $
-- $LastChangedRevision: 9216 $

------------------------------------------------------------------------------


-- Original DELETE removed: DELETE {{ bteq_var("DDSTG") }}.ACCT_BALN_BKDT_ADJ_RULE;
-- DBT handles table replacement via materialization strategy


-- Original INSERT converted to SELECT for DBT intermediate model
SELECT 
DT1.ACCT_I,
DT1.SRCE_SYST_C, 
DT1.BALN_TYPE_C,
DT1.CALC_FUNC_C,
DT1.TIME_PERD_C,
DT1.ADJ_FROM_D,
CASE WHEN ((DT1.EFFT_D - DT1.ADJ_FROM_D) YEAR(4) TO MONTH) (INTERVAL MONTH(4))  = 0 
THEN DT1.ADJ_FROM_D 
/*Backdated logic calculation when diffrence of months is 1 
and DT1.EFFT_D is between Business day 1 and Biz day 4*/
WHEN ((DT1.EFFT_D - DT1.ADJ_FROM_D) YEAR(4) TO MONTH) (INTERVAL MONTH(4))  = 1 
AND DT1.EFFT_D <= BSDY_4.CALR_CALR_D THEN DT1.ADJ_FROM_D

/*Backdated logic calculation when diffrence of months is 1 
and DT1.EFFT_D is NOT between Business day 1 and Biz day 4*/
WHEN ((DT1.EFFT_D - DT1.ADJ_FROM_D) YEAR(4) TO MONTH) (INTERVAL MONTH(4)) = 1 
AND DT1.EFFT_D > BSDY_4.CALR_CALR_D  THEN  DT1.EFFT_D - (EXTRACT (DAY FROM DT1.EFFT_D) - 1)  

/*Backdated logic calculation when diffrence of months is greater than 1 
and DT1.EFFT_D is between Business day 1 and Biz day 4*/
WHEN ((DT1.EFFT_D - DT1.ADJ_FROM_D) YEAR(4) TO MONTH) (INTERVAL MONTH(4))> 1 
AND DT1.EFFT_D <= BSDY_4.CALR_CALR_D 
THEN DT1.EFFT_D - (EXTRACT (DAY FROM DT1.EFFT_D) - 1)-INTERVAL '1' MONTH

/*Backdated logic calculation when diffrence of months is greater than 1 
and DT1.EFFT_D is  NOT between Business day 1 and Biz day 4*/
WHEN ((DT1.EFFT_D - DT1.ADJ_FROM_D) YEAR(4) TO MONTH) (INTERVAL MONTH(4))  > 1 
AND DT1.EFFT_D > BSDY_4.CALR_CALR_D  THEN DT1.EFFT_D - (EXTRACT (DAY FROM DT1.EFFT_D) - 1) 
END AS BKDT_ADJ_FROM_D,
DT1.ADJ_TO_D,
/*Similar adjustments for the same period are added */
SUM(DT1.ADJ_A) AS ADJ_A,
DT1.EFFT_D,
DT1.Gl_RECN_F,
DT1.PROS_KEY_EFFT_I
FROM
(
SELECT	
ADJ.ACCT_I AS ACCT_I,
ADJ.SRCE_SYST_C AS SRCE_SYST_C, 
ADJ.BALN_TYPE_C AS BALN_TYPE_C,
ADJ.CALC_FUNC_C AS CALC_FUNC_C,
ADJ.TIME_PERD_C AS TIME_PERD_C,
ADJ.ADJ_FROM_D AS ADJ_FROM_D,
ADJ.ADJ_TO_D,
/*Adjustments impacting the current record need to be loaded on the next day to avoid changing the open balances
*/
(CASE WHEN ADJ.EFFT_D = ADJ.ADJ_TO_D THEN EFFT_D+1
ELSE EFFT_D END) AS EFFT_D,
ADJ.Gl_RECN_F,
ADJ_A,
PROS_KEY_EFFT_I
FROM
{{ bteq_var("VTECH") }}.ACCT_BALN_ADJ  ADJ
WHERE	
SRCE_SYST_C = 'SAP'
AND BALN_TYPE_C='BALN'
AND CALC_FUNC_C='SPOT' 
AND TIME_PERD_C = 'E' 
/*Excluding the adjustments  with $0 in value as this brings no change to the 
$value in tha ACCT BALN and had a negative impact on the last records in 
ACCT BALN, so considerably important to eliminate*/
AND ADJ.ADJ_A <> 0 
/* Capturing delta adjustments*/
AND ADJ.EFFT_D >= 
	(SELECT MAX(BTCH_RUN_D) 
	FROM {{ bteq_var("VTECH") }}.UTIL_PROS_ISAC 
	WHERE    TRGT_M='ACCT_BALN_BKDT' AND SRCE_SYST_M='GDW'
	AND COMT_F = 'Y'  	AND SUCC_F='Y')
)DT1
INNER JOIN
(
/*Calulation of Business day 4 Logic*/
SELECT	
CALR_YEAR_N,
CALR_MNTH_N,
CALR_CALR_D
FROM	
{{ bteq_var("VTECH") }}.GRD_RPRT_CALR_CLYR
WHERE	
CALR_WEEK_DAY_N NOT IN (1,7) 
AND CALR_NON_WORK_DAY_F = 'N'
AND CALR_CALR_D BETWEEN  DATEADD(MONTH, -13, CURRENT_DATE) AND DATEADD(MONTH, +1, CURRENT_DATE)
QUALIFY	ROW_NUMBER() OVER (PARTITION BY CALR_YEAR_N, CALR_MNTH_N 
ORDER	BY CALR_CALR_D) = 4
)BSDY_4
ON EXTRACT (YEAR 
FROM DT1.EFFT_D)=EXTRACT (YEAR FROM BSDY_4.CALR_CALR_D)
AND EXTRACT (MONTH FROM DT1.EFFT_D)=EXTRACT (MONTH FROM BSDY_4.CALR_CALR_D)

WHERE
/*Including the adjustments that are excluded  in the previous run  for open record*/
DT1.EFFT_D <= (SELECT MAX(BTCH_RUN_D) AS BTCH_RUN_D
FROM {{ bteq_var("VTECH") }}.UTIL_PROS_ISAC
WHERE    TRGT_M='ACCT_BALN_ADJ' AND SRCE_SYST_M='SAP'
AND COMT_F = 'Y'  AND SUCC_F='Y')

/*To avoid any records that are processed in the previous runs */
AND  DT1.EFFT_D > (SELECT MAX(BTCH_RUN_D) AS BTCH_RUN_D
FROM {{ bteq_var("VTECH") }}.UTIL_PROS_ISAC
WHERE    
TRGT_M='ACCT_BALN_BKDT' AND SRCE_SYST_M='GDW'
AND COMT_F = 'Y'  AND SUCC_F='Y')

/*To exclude any adjustments that fall in the period where the GL is closed*/
AND BKDT_ADJ_FROM_D <= ADJ_TO_D

GROUP BY ACCT_I,SRCE_SYST_C, BALN_TYPE_C ,CALC_FUNC_C,TIME_PERD_C,ADJ_FROM_D,
BKDT_ADJ_FROM_D,ADJ_TO_D,EFFT_D,Gl_RECN_F, PROS_KEY_EFFT_I;




