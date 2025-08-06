{{ config(materialized='view', tags=['DltAppt_Pdct_RpayFrmTMP']) }}

WITH 
tmp_appt_pdct_rpay AS (
	SELECT
	*
	FROM {{ ref("tmp_appt_pdct_rpay")  }}),
appt_pdct_rpay AS (
	SELECT
	*
	FROM {{ ref("appt_pdct_rpay")  }}),
Appt_Pdct_Rpay_Tgt AS (SELECT APPT_PDCT_I, EFFT_D, SRCE_SYST_C, RPAY_SRCE_C, RPAY_SRCE_OTHR_X FROM APPT_PDCT_RPAY WHERE APPT_PDCT_I IN (SELECT DISTINCT APPT_PDCT_I FROM TMP_APPT_PDCT_RPAY WHERE RUN_STRM = 'CSE_COM_CPO_BUS_NCPR_CLNT') AND EXPY_D = '9999-12-31')


SELECT * FROM Appt_Pdct_Rpay_Tgt