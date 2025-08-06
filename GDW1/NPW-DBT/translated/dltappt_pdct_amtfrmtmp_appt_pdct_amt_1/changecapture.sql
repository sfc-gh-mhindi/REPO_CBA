{{ config(materialized='view', tags=['DltAPPT_PDCT_AMTFrmTMP_APPT_PDCT_AMT_1']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }}.NEW_APPT_PDCT_I, {{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }}.NEW_APPT_PDCT_I) AS NEW_APPT_PDCT_I,
		COALESCE({{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }}.NEW_AMT_TYPE_C, {{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }}.NEW_AMT_TYPE_C) AS NEW_AMT_TYPE_C,
		COALESCE({{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }}.NEW_APPT_PDCT_A, {{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }}.NEW_APPT_PDCT_A) AS NEW_APPT_PDCT_A,
		CASE
			WHEN {{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }}.NEW_APPT_PDCT_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }}.NEW_APPT_PDCT_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }}.NEW_APPT_PDCT_I) AND ({{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }}.NEW_AMT_TYPE_C <> {{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }}.NEW_AMT_TYPE_C OR {{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }}.NEW_APPT_PDCT_A <> {{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }}.NEW_APPT_PDCT_A) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }}.NEW_APPT_PDCT_I AND {{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }}.NEW_AMT_TYPE_C = {{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }}.NEW_AMT_TYPE_C AND {{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }}.NEW_APPT_PDCT_A = {{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }}.NEW_APPT_PDCT_A THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }} 
	ON {{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctAmtEntSeq') }}') }}.NEW_APPT_PDCT_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture