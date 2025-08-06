{{ config(materialized='view', tags=['DltAPPT_PDCT_ACCT_FrmTMP_APPT_PDCT_ACCT']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_APPT_PDCT_I, {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_APPT_PDCT_I) AS NEW_APPT_PDCT_I,
		COALESCE({{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_ACCT_I, {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_ACCT_I) AS NEW_ACCT_I,
		CASE
			WHEN {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_APPT_PDCT_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_APPT_PDCT_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_APPT_PDCT_I) AND ({{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_ACCT_I <> {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_ACCT_I) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_APPT_PDCT_I AND {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_ACCT_I = {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_ACCT_I THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }} 
	ON {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctAcctEntSeq') }}') }}.NEW_APPT_PDCT_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture