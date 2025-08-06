{{ config(materialized='view', tags=['DltAPPT_PDCT_RELFrmTMP_APPT_PDCT_REL']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_APPT_PDCT_I, {{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_APPT_PDCT_I) AS NEW_APPT_PDCT_I,
		COALESCE({{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_RELD_APPT_PDCT_I, {{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_RELD_APPT_PDCT_I) AS NEW_RELD_APPT_PDCT_I,
		COALESCE({{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_REL_TYPE_C, {{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_REL_TYPE_C) AS NEW_REL_TYPE_C,
		CASE
			WHEN {{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_APPT_PDCT_I IS NULL AND {{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_RELD_APPT_PDCT_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_APPT_PDCT_I IS NULL AND {{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_RELD_APPT_PDCT_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_APPT_PDCT_I AND {{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_RELD_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_RELD_APPT_PDCT_I) AND ({{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_REL_TYPE_C <> {{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_REL_TYPE_C) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_APPT_PDCT_I AND {{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_RELD_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_RELD_APPT_PDCT_I AND {{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_REL_TYPE_C = {{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_REL_TYPE_C THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptPdctRel') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptPdctRel') }}') }} 
	ON {{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_APPT_PDCT_I
	AND {{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_RELD_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctRel') }}') }}.NEW_RELD_APPT_PDCT_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture