{{ config(materialized='view', tags=['DltAPPTFrmTMP_APPT']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_I, {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_I) AS NEW_APPT_I,
		COALESCE({{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_C, {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_C) AS NEW_APPT_C,
		COALESCE({{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_FORM_C, {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_FORM_C) AS NEW_APPT_FORM_C,
		COALESCE({{ ref('{{ ref('CpyAppt') }}') }}.NEW_STUS_TRAK_I, {{ ref('{{ ref('CpyAppt') }}') }}.NEW_STUS_TRAK_I) AS NEW_STUS_TRAK_I,
		COALESCE({{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_ORIG_C, {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_ORIG_C) AS NEW_APPT_ORIG_C,
		CASE
			WHEN {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_I) AND ({{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_C <> {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_C OR {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_FORM_C <> {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_FORM_C OR {{ ref('{{ ref('CpyAppt') }}') }}.NEW_STUS_TRAK_I <> {{ ref('{{ ref('CpyAppt') }}') }}.NEW_STUS_TRAK_I OR {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_ORIG_C <> {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_ORIG_C) THEN '3'
			WHEN {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_C = {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_C AND {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_FORM_C = {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_FORM_C AND {{ ref('{{ ref('CpyAppt') }}') }}.NEW_STUS_TRAK_I = {{ ref('{{ ref('CpyAppt') }}') }}.NEW_STUS_TRAK_I AND {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_ORIG_C = {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_ORIG_C THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyAppt') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyAppt') }}') }} 
	ON {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture