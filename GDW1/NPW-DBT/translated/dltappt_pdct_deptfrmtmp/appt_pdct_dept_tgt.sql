{{ config(materialized='view', tags=['DltAppt_Pdct_DeptFrmTMP']) }}

WITH 
appt_pdct_dept AS (
	SELECT
	*
	FROM {{ ref("appt_pdct_dept")  }}),
tmp_appt_pdct_dept AS (
	SELECT
	*
	FROM {{ ref("tmp_appt_pdct_dept")  }}),
Appt_Pdct_Dept_Tgt AS (SELECT APPT_PDCT_I, BRCH_N, DEPT_I, DEPT_ROLE_C, SRCE_SYST_C, EFFT_D FROM APPT_PDCT_DEPT WHERE APPT_PDCT_I IN (SELECT DISTINCT APPT_PDCT_I FROM TMP_APPT_PDCT_DEPT WHERE RUN_STRM = 'CSE_COM_CPO_BUS_NCPR_CLNT') AND EXPY_D = '9999-12-31')


SELECT * FROM Appt_Pdct_Dept_Tgt