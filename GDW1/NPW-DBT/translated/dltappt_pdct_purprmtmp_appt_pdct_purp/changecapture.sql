{{ config(materialized='view', tags=['DltAPPT_PDCT_PURPrmTMP_APPT_PDCT_PURP']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_APPT_PDCT_I, {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_APPT_PDCT_I) AS NEW_APPT_PDCT_I,
		COALESCE({{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I, {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I) AS NEW_SRCE_SYST_APPT_PDCT_PURP_I,
		COALESCE({{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_TYPE_C, {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_TYPE_C) AS NEW_PURP_TYPE_C,
		COALESCE({{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_CLAS_C, {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_CLAS_C) AS NEW_PURP_CLAS_C,
		COALESCE({{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_C, {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_C) AS NEW_SRCE_SYST_C,
		COALESCE({{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_A, {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_A) AS NEW_PURP_A,
		COALESCE({{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_CNCY_C, {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_CNCY_C) AS NEW_CNCY_C,
		COALESCE({{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_MAIN_PURP_F, {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_MAIN_PURP_F) AS NEW_MAIN_PURP_F,
		CASE
			WHEN {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_APPT_PDCT_I IS NULL AND {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_APPT_PDCT_I IS NULL AND {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_APPT_PDCT_I AND {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I = {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I) AND ({{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_TYPE_C <> {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_TYPE_C OR {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_CLAS_C <> {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_CLAS_C OR {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_C <> {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_C OR {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_A <> {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_A OR {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_CNCY_C <> {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_CNCY_C OR {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_MAIN_PURP_F <> {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_MAIN_PURP_F) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_APPT_PDCT_I AND {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I = {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I AND {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_TYPE_C = {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_TYPE_C AND {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_CLAS_C = {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_CLAS_C AND {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_C = {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_C AND {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_A = {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_PURP_A AND {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_CNCY_C = {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_CNCY_C AND {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_MAIN_PURP_F = {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_MAIN_PURP_F THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptPdctPurp') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptPdctPurp') }}') }} 
	ON {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_APPT_PDCT_I
	AND {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I = {{ ref('{{ ref('CpyApptPdctPurp') }}') }}.NEW_SRCE_SYST_APPT_PDCT_PURP_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture