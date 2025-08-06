{{ config(materialized='view', tags=['DltAPPT_ASETFrmTMP_APPT_ASET']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_APPT_I, {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_APPT_I) AS NEW_APPT_I,
		COALESCE({{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ASET_I, {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ASET_I) AS NEW_ASET_I,
		CASE
			WHEN {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_APPT_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_APPT_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_APPT_I) AND ({{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ASET_I <> {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ASET_I) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ASET_I = {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_ASET_I THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptFeat') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptFeat') }}') }} 
	ON {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptFeat') }}') }}.NEW_APPT_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture