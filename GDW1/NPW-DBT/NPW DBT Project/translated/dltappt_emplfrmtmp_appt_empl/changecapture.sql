{{ config(materialized='view', tags=['DltAPPT_EMPLFrmTMP_APPT_EMPL']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_APPT_I, {{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_APPT_I) AS NEW_APPT_I,
		COALESCE({{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_EMPL_ROLE_C, {{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_EMPL_ROLE_C) AS NEW_EMPL_ROLE_C,
		COALESCE({{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_EMPL_I, {{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_EMPL_I) AS NEW_EMPL_I,
		CASE
			WHEN {{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_APPT_I IS NULL AND {{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_EMPL_ROLE_C IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_APPT_I IS NULL AND {{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_EMPL_ROLE_C IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_EMPL_ROLE_C = {{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_EMPL_ROLE_C) AND ({{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_EMPL_I <> {{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_EMPL_I) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_EMPL_ROLE_C = {{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_EMPL_ROLE_C AND {{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_EMPL_I = {{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_EMPL_I THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptEmpl') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptEmpl') }}') }} 
	ON {{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_APPT_I
	AND {{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_EMPL_ROLE_C = {{ ref('{{ ref('CpyApptEmpl') }}') }}.NEW_EMPL_ROLE_C
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture