{{ config(materialized='view', tags=['DltAPPT_PDCT_UNID_PATY_FrmTMP_APPT_PDCT_UNID_PATY']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_APPT_PDCT_I, {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_APPT_PDCT_I) AS NEW_APPT_PDCT_I,
		COALESCE({{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_SRCE_SYST_PATY_I, {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_SRCE_SYST_PATY_I) AS NEW_SRCE_SYST_PATY_I,
		COALESCE({{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_PATY_M, {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_PATY_M) AS NEW_PATY_M,
		CASE
			WHEN {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_APPT_PDCT_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_APPT_PDCT_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_APPT_PDCT_I) AND ({{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_PATY_M <> {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_PATY_M OR {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_SRCE_SYST_PATY_I <> {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_SRCE_SYST_PATY_I) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_APPT_PDCT_I AND {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_PATY_M = {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_PATY_M AND {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_SRCE_SYST_PATY_I = {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_SRCE_SYST_PATY_I THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptPdctFeat') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptPdctFeat') }}') }} 
	ON {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctFeat') }}') }}.NEW_APPT_PDCT_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture