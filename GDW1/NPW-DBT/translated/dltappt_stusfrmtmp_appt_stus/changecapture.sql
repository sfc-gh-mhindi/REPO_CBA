{{ config(materialized='view', tags=['DltAPPT_STUSFrmTMP_APPT_STUS']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptStus') }}') }}.NEW_APPT_I, {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_APPT_I) AS NEW_APPT_I,
		COALESCE({{ ref('{{ ref('CpyApptStus') }}') }}.NEW_STUS_C, {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_STUS_C) AS NEW_STUS_C,
		COALESCE({{ ref('{{ ref('CpyApptStus') }}') }}.NEW_STRT_S, {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_STRT_S) AS NEW_STRT_S,
		COALESCE({{ ref('{{ ref('CpyApptStus') }}') }}.NEW_END_D, {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_END_D) AS NEW_END_D,
		COALESCE({{ ref('{{ ref('CpyApptStus') }}') }}.NEW_END_T, {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_END_T) AS NEW_END_T,
		COALESCE({{ ref('{{ ref('CpyApptStus') }}') }}.NEW_END_S, {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_END_S) AS NEW_END_S,
		CASE
			WHEN {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_APPT_I IS NULL AND {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_STUS_C IS NULL AND {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_STRT_S IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_APPT_I IS NULL AND {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_STUS_C IS NULL AND {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_STRT_S IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptStus') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_STUS_C = {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_STUS_C AND {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_STRT_S = {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_STRT_S) AND ({{ ref('{{ ref('CpyApptStus') }}') }}.NEW_END_S <> {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_END_S) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_STUS_C = {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_STUS_C AND {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_STRT_S = {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_STRT_S AND {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_END_S = {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_END_S THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptStus') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptStus') }}') }} 
	ON {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_APPT_I
	AND {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_STUS_C = {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_STUS_C
	AND {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_STRT_S = {{ ref('{{ ref('CpyApptStus') }}') }}.NEW_STRT_S
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture