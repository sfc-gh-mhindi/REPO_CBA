{{ config(materialized='view', tags=['DltAppt_Pdct_PurpfrmTMP_tran']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_APPT_PDCT_I, {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_APPT_PDCT_I) AS NEW_APPT_PDCT_I,
		COALESCE({{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_C, {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_C) AS NEW_SRCE_SYST_C,
		COALESCE({{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I, {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I) AS NEW_SRCE_SYST_APPT_PDCT_PURP_I,
		COALESCE({{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_TYPE_C, {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_TYPE_C) AS NEW_PURP_TYPE_C,
		CASE
			WHEN {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_APPT_PDCT_I IS NULL AND {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_C IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_APPT_PDCT_I IS NULL AND {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_C IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_APPT_PDCT_I AND {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_C = {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_C) AND ({{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_TYPE_C <> {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_TYPE_C OR {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I <> {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_APPT_PDCT_I AND {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_C = {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_C AND {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_TYPE_C = {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_TYPE_C AND {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I = {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptPdctPurp') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptPdctPurp') }}') }} 
	ON {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_APPT_PDCT_I
	AND {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_C = {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_C
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture