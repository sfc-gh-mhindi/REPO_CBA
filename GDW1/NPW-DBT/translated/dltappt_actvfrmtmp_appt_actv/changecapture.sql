{{ config(materialized='view', tags=['DltAPPT_ACTVFrmTMP_APPT_ACTV']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptActv') }}') }}.NEW_APPT_I, {{ ref('{{ ref('CpyApptActv') }}') }}.NEW_APPT_I) AS NEW_APPT_I,
		CASE
			WHEN {{ ref('{{ ref('CpyApptActv') }}') }}.NEW_APPT_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptActv') }}') }}.NEW_APPT_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptActv') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptActv') }}') }}.NEW_APPT_I) AND ({{ ref('{{ ref('CpyApptActv') }}') }}.NEW_APPT_ACTV_Q <> {{ ref('{{ ref('CpyApptActv') }}') }}.NEW_APPT_ACTV_Q) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptActv') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptActv') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptActv') }}') }}.NEW_APPT_ACTV_Q = {{ ref('{{ ref('CpyApptActv') }}') }}.NEW_APPT_ACTV_Q THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptActv') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptActv') }}') }} 
	ON {{ ref('{{ ref('CpyApptActv') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptActv') }}') }}.NEW_APPT_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture