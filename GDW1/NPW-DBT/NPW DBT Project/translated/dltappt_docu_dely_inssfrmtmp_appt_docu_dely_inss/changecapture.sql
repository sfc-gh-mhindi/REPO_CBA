{{ config(materialized='view', tags=['DltAPPT_DOCU_DELY_INSSFrmTMP_APPT_DOCU_DELY_INSS']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptDocuDelyInss') }}') }}.NEW_APPT_I, {{ ref('{{ ref('CpyApptDocuDelyInss') }}') }}.NEW_APPT_I) AS NEW_APPT_I,
		CASE
			WHEN {{ ref('{{ ref('CpyApptDocuDelyInss') }}') }}.NEW_APPT_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptDocuDelyInss') }}') }}.NEW_APPT_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptDocuDelyInss') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptDocuDelyInss') }}') }}.NEW_APPT_I) AND ({{ ref('{{ ref('CpyApptDocuDelyInss') }}') }}.NEW_DOCU_DELY_RECV_C <> {{ ref('{{ ref('CpyApptDocuDelyInss') }}') }}.NEW_DOCU_DELY_RECV_C) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptDocuDelyInss') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptDocuDelyInss') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptDocuDelyInss') }}') }}.NEW_DOCU_DELY_RECV_C = {{ ref('{{ ref('CpyApptDocuDelyInss') }}') }}.NEW_DOCU_DELY_RECV_C THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptDocuDelyInss') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptDocuDelyInss') }}') }} 
	ON {{ ref('{{ ref('CpyApptDocuDelyInss') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptDocuDelyInss') }}') }}.NEW_APPT_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture