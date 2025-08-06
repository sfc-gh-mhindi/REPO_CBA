{{ config(materialized='view', tags=['DltAppt_StusFrmTMP']) }}

WITH 
appt_stus AS (
	SELECT
	*
	FROM {{ ref("appt_stus")  }}),
tmp_appt_stus AS (
	SELECT
	*
	FROM {{ ref("tmp_appt_stus")  }}),
Appt_Stus_Tgt AS (SELECT TRIM(APPT_I), CAST(STUS_C AS CHAR(4)), STRT_S, STRT_D, STRT_T, END_D, END_T, END_S, TRIM(EMPL_I), EFFT_D FROM APPT_STUS WHERE TRIM(APPT_I) IN (SELECT DISTINCT APPT_I FROM TMP_APPT_STUS WHERE RUN_STRM = 'CSE_COM_CPO_BUS_NCPR_CLNT') AND EXPY_D = '9999-12-31')


SELECT * FROM Appt_Stus_Tgt