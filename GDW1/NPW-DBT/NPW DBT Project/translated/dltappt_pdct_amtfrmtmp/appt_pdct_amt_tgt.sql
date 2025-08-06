{{ config(materialized='view', tags=['DltAppt_Pdct_AmtFrmTMP']) }}

WITH 
appt_pdct_amt AS (
	SELECT
	*
	FROM {{ ref("appt_pdct_amt")  }}),
tmp_appt_pdct_amt AS (
	SELECT
	*
	FROM {{ ref("tmp_appt_pdct_amt")  }}),
Appt_Pdct_Amt_Tgt AS (SELECT APPT_PDCT_I, AMT_TYPE_C, CNCY_C, APPT_PDCT_A, XCES_AMT_REAS_X, EFFT_D, COALESCE(SRCE_SYST_C, '') AS SRCE_SYST_C FROM APPT_PDCT_AMT WHERE APPT_PDCT_I IN (SELECT DISTINCT APPT_PDCT_I FROM TMP_APPT_PDCT_AMT) AND EXPY_D = '9999-12-31')


SELECT * FROM Appt_Pdct_Amt_Tgt