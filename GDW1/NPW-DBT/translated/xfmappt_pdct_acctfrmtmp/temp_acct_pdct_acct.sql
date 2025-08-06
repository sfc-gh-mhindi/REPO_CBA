{{ config(materialized='view', tags=['XfmAppt_Pdct_AcctFrmTmp']) }}

WITH 
appt_pdct_acct_temp AS (
	SELECT
	*
	FROM {{ ref("appt_pdct_acct_temp")  }}),
locn_serv_curr AS (
	SELECT
	*
	FROM {{ source("svodcse","locn_serv_curr")  }}),
Temp_Acct_Pdct_Acct AS (SELECT ACCOUNT_NUMBER, REPAYMENT_ACCOUNT_NUMBER, APPT_PDCT_I, REL_TYPE_C, EFFT_D, PROS_KEY_EFFT_I, b.ACCT_I AS ACCT_I, ERR_FLG, PROS_KEY_EXPY_I, EXPY_D FROM APPT_PDCT_ACCT_TEMP LEFT OUTER JOIN (SELECT srce_syst_acct_numb, acct_i FROM locn_serv_curr) AS B ON a.REPAYMENT_ACCOUNT_NUMBER = b.srce_syst_acct_numb WHERE a.rel_type_c = 'RPAY' UNION ALL SELECT ACCOUNT_NUMBER, REPAYMENT_ACCOUNT_NUMBER, APPT_PDCT_I, REL_TYPE_C, EFFT_D, PROS_KEY_EFFT_I, b.ACCT_I AS ACCT_I, ERR_FLG, PROS_KEY_EXPY_I, EXPY_D FROM APPT_PDCT_ACCT_TEMP LEFT OUTER JOIN (SELECT srce_syst_acct_numb, acct_i FROM locn_serv_curr) AS B ON a.ACCOUNT_NUMBER = b.srce_syst_acct_numb WHERE a.rel_type_c = 'OVDR')


SELECT * FROM Temp_Acct_Pdct_Acct