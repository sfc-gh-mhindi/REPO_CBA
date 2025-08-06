{{ config(materialized='view', tags=['DltAPPT_STUS_REASFrmTMP_APPT_STUS_REAS']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_APPT_I, {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_APPT_I) AS NEW_APPT_I,
		COALESCE({{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STUS_C, {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STUS_C) AS NEW_STUS_C,
		COALESCE({{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STUS_REAS_TYPE_C, {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STUS_REAS_TYPE_C) AS NEW_STUS_REAS_TYPE_C,
		COALESCE({{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STRT_S, {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STRT_S) AS NEW_STRT_S,
		COALESCE({{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_END_S, {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_END_S) AS NEW_END_S,
		CASE
			WHEN {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_APPT_I IS NULL AND {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STUS_C IS NULL AND {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STUS_REAS_TYPE_C IS NULL AND {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STRT_S IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_APPT_I IS NULL AND {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STUS_C IS NULL AND {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STUS_REAS_TYPE_C IS NULL AND {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STRT_S IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STUS_C = {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STUS_C AND {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STUS_REAS_TYPE_C = {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STUS_REAS_TYPE_C AND {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STRT_S = {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STRT_S) AND ({{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_END_S <> {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_END_S) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STUS_C = {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STUS_C AND {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STUS_REAS_TYPE_C = {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STUS_REAS_TYPE_C AND {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STRT_S = {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STRT_S AND {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_END_S = {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_END_S THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptStusReas') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptStusReas') }}') }} 
	ON {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_APPT_I
	AND {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STUS_C = {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STUS_C
	AND {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STUS_REAS_TYPE_C = {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STUS_REAS_TYPE_C
	AND {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STRT_S = {{ ref('{{ ref('CpyApptStusReas') }}') }}.NEW_STRT_S
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture