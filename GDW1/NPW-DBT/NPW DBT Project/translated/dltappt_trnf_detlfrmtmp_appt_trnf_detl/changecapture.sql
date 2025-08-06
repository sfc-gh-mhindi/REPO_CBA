{{ config(materialized='view', tags=['DltAPPT_TRNF_DETLFrmTMP_APPT_TRNF_DETL']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_APPT_I, {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_APPT_I) AS NEW_APPT_I,
		COALESCE({{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_APPT_TRNF_I, {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_APPT_TRNF_I) AS NEW_APPT_TRNF_I,
		COALESCE({{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_TRNF_OPTN_C, {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_TRNF_OPTN_C) AS NEW_TRNF_OPTN_C,
		COALESCE({{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_TRNF_A, {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_TRNF_A) AS NEW_TRNF_A,
		COALESCE({{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_CMPE_I, {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_CMPE_I) AS NEW_CMPE_I,
		CASE
			WHEN {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_APPT_I IS NULL AND {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_APPT_TRNF_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_APPT_I IS NULL AND {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_APPT_TRNF_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_APPT_TRNF_I = {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_APPT_TRNF_I) AND ({{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_TRNF_OPTN_C <> {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_TRNF_OPTN_C OR {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_TRNF_A <> {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_TRNF_A OR {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_CMPE_I <> {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_CMPE_I) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_APPT_TRNF_I = {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_APPT_TRNF_I AND {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_TRNF_OPTN_C = {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_TRNF_OPTN_C AND {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_TRNF_A = {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_TRNF_A AND {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_CMPE_I = {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_CMPE_I THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }} 
	ON {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_APPT_I
	AND {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_APPT_TRNF_I = {{ ref('{{ ref('CpyApptTrnfDetlEntSeq') }}') }}.NEW_APPT_TRNF_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture