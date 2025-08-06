{{ config(materialized='view', tags=['DltAppt_DeptFrmTMP_XFM']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_APPT_I, {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_APPT_I) AS NEW_APPT_I,
		COALESCE({{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_DEPT_ROLE_C, {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_DEPT_ROLE_C) AS NEW_DEPT_ROLE_C,
		COALESCE({{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_DEPT_I, {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_DEPT_I) AS NEW_DEPT_I,
		CASE
			WHEN {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_APPT_I IS NULL AND {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_DEPT_ROLE_C IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_APPT_I IS NULL AND {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_DEPT_ROLE_C IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_DEPT_ROLE_C = {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_DEPT_ROLE_C) AND ({{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_DEPT_I <> {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_DEPT_I) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_DEPT_ROLE_C = {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_DEPT_ROLE_C AND {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_DEPT_I = {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_DEPT_I THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }} 
	ON {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_APPT_I
	AND {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_DEPT_ROLE_C = {{ ref('{{ ref('CpyApptDeptEntSeq') }}') }}.NEW_DEPT_ROLE_C
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture