{{ config(materialized='view', tags=['DltAPPT_RELFrmTMP_APPT_REL']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptRel') }}') }}.NEW_APPT_I, {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_APPT_I) AS NEW_APPT_I,
		COALESCE({{ ref('{{ ref('CpyApptRel') }}') }}.NEW_RELD_APPT_I, {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_RELD_APPT_I) AS NEW_RELD_APPT_I,
		COALESCE({{ ref('{{ ref('CpyApptRel') }}') }}.NEW_REL_TYPE_C, {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_REL_TYPE_C) AS NEW_REL_TYPE_C,
		CASE
			WHEN {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_APPT_I IS NULL AND {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_RELD_APPT_I IS NULL AND {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_REL_TYPE_C IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_APPT_I IS NULL AND {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_RELD_APPT_I IS NULL AND {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_REL_TYPE_C IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptRel') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_RELD_APPT_I = {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_RELD_APPT_I AND {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_REL_TYPE_C = {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_REL_TYPE_C) AND (NULL) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_RELD_APPT_I = {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_RELD_APPT_I AND {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_REL_TYPE_C = {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_REL_TYPE_C THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptRel') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptRel') }}') }} 
	ON {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_APPT_I
	AND {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_RELD_APPT_I = {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_RELD_APPT_I
	AND {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_REL_TYPE_C = {{ ref('{{ ref('CpyApptRel') }}') }}.NEW_REL_TYPE_C
	WHERE change_code = '1'
)

SELECT * FROM ChangeCapture