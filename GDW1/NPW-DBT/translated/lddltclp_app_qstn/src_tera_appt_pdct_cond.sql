{{ config(materialized='view', tags=['LdDltClp_App_Qstn']) }}

WITH 
appt_qstn AS (
	SELECT
	*
	FROM {{ ref("appt_qstn")  }}),
tmp_appt_qstn AS (
	SELECT
	*
	FROM {{ ref("tmp_appt_qstn")  }}),
Src_Tera_APPT_PDCT_COND AS (SELECT a.APPT_I AS NEW_APPT_I, a.QSTN_C AS NEW_QSTN_C, A.RESP_C AS NEW_RESP_C, a.RESP_CMMT_X AS NEW_RESP_CMMT_X, a.paty_i AS NEW_PATY_I, COALESCE(A.ROW_SECU_ACCS_C, 0) AS NEW_ROW_SECU_ACCS_C, b.APPT_I AS OLD_APPT_I, b.QSTN_C AS OLD_QSTN_C, b.RESP_C AS OLD_RESP_C, b.RESP_CMMT_X AS OLD_RESP_CMMT_X, b.paty_i AS OLD_PATY_I FROM TMP_APPT_QSTN LEFT OUTER JOIN APPT_QSTN ON a.APPT_I = b.APPT_I AND a.QSTN_C = B.QSTN_C AND TRIM(COALESCE(a.paty_i, 'X')) = TRIM(COALESCE(b.paty_i, 'X')) AND b.EXPY_D = '9999-12-31')


SELECT * FROM Src_Tera_APPT_PDCT_COND