{{ config(materialized='view', tags=['DltAPPT_FEAT_SHLFrmTMP_APPT_FEAT']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_APPT_I, {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_APPT_I) AS NEW_APPT_I,
		COALESCE({{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_FEAT_I, {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_FEAT_I) AS NEW_FEAT_I,
		COALESCE({{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_SHL_F, {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_SHL_F) AS NEW_SHL_F,
		CASE
			WHEN {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_APPT_I IS NULL AND {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_FEAT_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_APPT_I IS NULL AND {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_FEAT_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_FEAT_I = {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_FEAT_I) AND ({{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_SHL_F <> {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_SHL_F) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_FEAT_I = {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_FEAT_I AND {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_SHL_F = {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_SHL_F THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptFeat') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptFeat') }}') }} 
	ON {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_APPT_I
	AND {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_FEAT_I = {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_FEAT_I
	WHERE change_code = '1' OR change_code = '3' OR change_code = '0'
)

SELECT * FROM ChangeCapture