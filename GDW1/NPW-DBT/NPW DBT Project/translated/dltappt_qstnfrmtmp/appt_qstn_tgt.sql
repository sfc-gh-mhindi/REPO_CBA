{{ config(materialized='view', tags=['DltAppt_QstnFrmTMP']) }}

WITH 
appt_qstn AS (
	SELECT
	*
	FROM {{ ref("appt_qstn")  }}),
tmp_appt_qstn AS (
	SELECT
	*
	FROM {{ ref("tmp_appt_qstn")  }}),
Appt_Qstn_Tgt AS (SELECT TRIM(APPT_I) AS APPT_I, QSTN_C, RESP_C, TRIM(RESP_CMMT_X) AS RESP_CMMT_X, TRIM(PATY_I) AS PATY_I, EFFT_D, EXPY_D, PROS_KEY_EFFT_I, PROS_KEY_EXPY_I FROM APPT_QSTN WHERE APPT_I IN (SELECT DISTINCT TRIM(APPT_I) FROM TMP_APPT_QSTN) AND EXPY_D = '9999-12-31')


SELECT * FROM Appt_Qstn_Tgt