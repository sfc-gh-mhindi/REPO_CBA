{{ config(materialized='view', tags=['DltAPPT_PATYFrmTMP_APPT_PATY']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_APPT_I, {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_APPT_I) AS NEW_APPT_I,
		COALESCE({{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_REL_C, {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_REL_C) AS NEW_REL_C,
		COALESCE({{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_PATY_I, {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_PATY_I) AS NEW_PATY_I,
		CASE
			WHEN {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_APPT_I IS NULL AND {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_PATY_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_APPT_I IS NULL AND {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_PATY_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_PATY_I = {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_PATY_I) AND ({{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_REL_C <> {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_REL_C) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_PATY_I = {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_PATY_I AND {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_REL_C = {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_REL_C THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }} 
	ON {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_APPT_I
	AND {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_PATY_I = {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_PATY_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture