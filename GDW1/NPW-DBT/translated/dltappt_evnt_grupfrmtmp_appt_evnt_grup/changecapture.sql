{{ config(materialized='view', tags=['DltAPPT_EVNT_GRUPFrmTMP_APPT_EVNT_GRUP']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyApptEvntGrup') }}') }}.NEW_APPT_I, {{ ref('{{ ref('CpyApptEvntGrup') }}') }}.NEW_APPT_I) AS NEW_APPT_I,
		COALESCE({{ ref('{{ ref('CpyApptEvntGrup') }}') }}.NEW_EVNT_GRUP_I, {{ ref('{{ ref('CpyApptEvntGrup') }}') }}.NEW_EVNT_GRUP_I) AS NEW_EVNT_GRUP_I,
		CASE
			WHEN {{ ref('{{ ref('CpyApptEvntGrup') }}') }}.NEW_APPT_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyApptEvntGrup') }}') }}.NEW_APPT_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyApptEvntGrup') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptEvntGrup') }}') }}.NEW_APPT_I) AND ({{ ref('{{ ref('CpyApptEvntGrup') }}') }}.NEW_EVNT_GRUP_I <> {{ ref('{{ ref('CpyApptEvntGrup') }}') }}.NEW_EVNT_GRUP_I) THEN '3'
			WHEN {{ ref('{{ ref('CpyApptEvntGrup') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptEvntGrup') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyApptEvntGrup') }}') }}.NEW_EVNT_GRUP_I = {{ ref('{{ ref('CpyApptEvntGrup') }}') }}.NEW_EVNT_GRUP_I THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyApptEvntGrup') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyApptEvntGrup') }}') }} 
	ON {{ ref('{{ ref('CpyApptEvntGrup') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyApptEvntGrup') }}') }}.NEW_APPT_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture