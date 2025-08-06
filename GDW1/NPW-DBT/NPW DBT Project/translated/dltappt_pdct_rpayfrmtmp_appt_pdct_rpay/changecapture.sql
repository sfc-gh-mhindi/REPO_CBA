{{ config(materialized='view', tags=['DltAPPT_PDCT_RPAYFrmTMP_APPT_PDCT_RPAY']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_APPT_PDCT_I, {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_APPT_PDCT_I) AS NEW_APPT_PDCT_I,
		COALESCE({{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_RPAY_TYPE_C, {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_RPAY_TYPE_C) AS NEW_RPAY_TYPE_C,
		COALESCE({{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_PAYT_FREQ_C, {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_PAYT_FREQ_C) AS NEW_PAYT_FREQ_C,
		CASE
			WHEN {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_APPT_PDCT_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_APPT_PDCT_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_APPT_PDCT_I) AND ({{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_RPAY_TYPE_C <> {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_RPAY_TYPE_C OR {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_PAYT_FREQ_C <> {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_PAYT_FREQ_C) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_APPT_PDCT_I AND {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_RPAY_TYPE_C = {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_RPAY_TYPE_C AND {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_PAYT_FREQ_C = {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_PAYT_FREQ_C THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptPdctFeat') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptPdctFeat') }}') }} 
	ON {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_APPT_PDCT_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture