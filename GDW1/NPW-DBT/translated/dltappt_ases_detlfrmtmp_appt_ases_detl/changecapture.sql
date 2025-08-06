{{ config(materialized='view', tags=['DltAPPT_ASES_DETLFrmTMP_APPT_ASES_DETL']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_APPT_I, {{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_APPT_I) AS NEW_APPT_I,
		COALESCE({{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_AMT_TYPE_C, {{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_AMT_TYPE_C) AS NEW_AMT_TYPE_C,
		COALESCE({{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_APPT_ASES_A, {{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_APPT_ASES_A) AS NEW_APPT_ASES_A,
		CASE
			WHEN {{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_APPT_I IS NULL AND {{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_AMT_TYPE_C IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_APPT_I IS NULL AND {{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_AMT_TYPE_C IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_AMT_TYPE_C = {{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_AMT_TYPE_C) AND ({{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_APPT_ASES_A <> {{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_APPT_ASES_A) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_AMT_TYPE_C = {{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_AMT_TYPE_C AND {{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_APPT_ASES_A = {{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_APPT_ASES_A THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptAsesDetl') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptAsesDetl') }}') }} 
	ON {{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_APPT_I
	AND {{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_AMT_TYPE_C = {{ ref('{{ ref('CpyApptAsesDetl') }}') }}.NEW_AMT_TYPE_C
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture