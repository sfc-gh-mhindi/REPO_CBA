{{ config(materialized='view', tags=['DltAPPT_PDCT_PATYFrmTMP_APPT_PDCT_PATY']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_APPT_PDCT_I, {{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_APPT_PDCT_I) AS NEW_APPT_PDCT_I,
		COALESCE({{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_PATY_ROLE_C, {{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_PATY_ROLE_C) AS NEW_PATY_ROLE_C,
		COALESCE({{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_PATY_I, {{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_PATY_I) AS NEW_PATY_I,
		CASE
			WHEN {{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_APPT_PDCT_I IS NULL AND {{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_PATY_ROLE_C IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_APPT_PDCT_I IS NULL AND {{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_PATY_ROLE_C IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_APPT_PDCT_I AND {{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_PATY_ROLE_C = {{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_PATY_ROLE_C) AND ({{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_PATY_I <> {{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_PATY_I) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_APPT_PDCT_I AND {{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_PATY_ROLE_C = {{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_PATY_ROLE_C AND {{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_PATY_I = {{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_PATY_I THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptPdctPaty') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptPdctPaty') }}') }} 
	ON {{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_APPT_PDCT_I = {{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_APPT_PDCT_I
	AND {{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_PATY_ROLE_C = {{ ref('{{ ref('CpyApptPdctPaty') }}') }}.NEW_PATY_ROLE_C
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture