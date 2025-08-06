{{ config(materialized='view', tags=['DltAppt_DeptFrmTMP_XFM']) }}

WITH 
tmp_appt_dept AS (
	SELECT
	*
	FROM {{ ref("tmp_appt_dept")  }}),
appt_dept AS (
	SELECT
	*
	FROM {{ ref("appt_dept")  }}),
SrcTmpApptDeptTera AS (SELECT a.APPT_I AS NEW_APPT_I, a.DEPT_ROLE_C AS NEW_DEPT_ROLE_C, a.DEPT_I AS NEW_DEPT_I, b.APPT_I AS OLD_APPT_I, b.DEPT_ROLE_C AS OLD_DEPT_ROLE_C, b.DEPT_I AS OLD_DEPT_I, b.EFFT_D AS OLD_EFFT_D FROM TMP_APPT_DEPT LEFT OUTER JOIN APPT_DEPT ON TRIM(a.APPT_I) = TRIM(b.APPT_I) AND TRIM(a.DEPT_ROLE_C) = TRIM(b.DEPT_ROLE_C) AND b.EXPY_D = '9999-12-31' WHERE a.RUN_STRM = '{{ var("RUN_STREAM") }}')


SELECT * FROM SrcTmpApptDeptTera