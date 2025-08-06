{{ config(materialized='view', tags=['DltAPPTTBPFrmTMP_APPT']) }}

WITH ChangeCapture AS (
	SELECT
		COALESCE({{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_I, {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_I) AS NEW_APPT_I,
		COALESCE({{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_C, {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_C) AS NEW_APPT_C,
		COALESCE({{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_FORM_C, {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_FORM_C) AS NEW_APPT_FORM_C,
		COALESCE({{ ref('{{ ref('CpyAppt') }}') }}.NEW_STUS_TRAK_I, {{ ref('{{ ref('CpyAppt') }}') }}.NEW_STUS_TRAK_I) AS NEW_STUS_TRAK_I,
		COALESCE({{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_ORIG_C, {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_ORIG_C) AS NEW_APPT_ORIG_C,
		COALESCE({{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_ORIG_SYST_C, {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_ORIG_SYST_C) AS NEW_APPT_ORIG_SYST_C,
		COALESCE({{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_RECV_S, {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_RECV_S) AS NEW_APPT_RECV_S,
		COALESCE({{ ref('{{ ref('CpyAppt') }}') }}.NEW_REL_MGR_STAT_C, {{ ref('{{ ref('CpyAppt') }}') }}.NEW_REL_MGR_STAT_C) AS NEW_REL_MGR_STAT_C,
		COALESCE({{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_RECV_D, {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_RECV_D) AS NEW_APPT_RECV_D,
		COALESCE({{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_RECV_T, {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_RECV_T) AS NEW_APPT_RECV_T,
		CASE
			WHEN {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_I IS NULL THEN '1'
			WHEN {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_I IS NULL THEN '2'
			WHEN ({{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_I) AND ({{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_C <> {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_C OR {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_FORM_C <> {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_FORM_C OR {{ ref('{{ ref('CpyAppt') }}') }}.NEW_STUS_TRAK_I <> {{ ref('{{ ref('CpyAppt') }}') }}.NEW_STUS_TRAK_I OR {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_ORIG_C <> {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_ORIG_C OR {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_ORIG_SYST_C <> {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_ORIG_SYST_C OR {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_RECV_S <> {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_RECV_S OR {{ ref('{{ ref('CpyAppt') }}') }}.NEW_REL_MGR_STAT_C <> {{ ref('{{ ref('CpyAppt') }}') }}.NEW_REL_MGR_STAT_C OR {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_RECV_D <> {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_RECV_D OR {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_RECV_T <> {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_RECV_T) THEN '3'
			WHEN {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_I AND {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_C = {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_C AND {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_FORM_C = {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_FORM_C AND {{ ref('{{ ref('CpyAppt') }}') }}.NEW_STUS_TRAK_I = {{ ref('{{ ref('CpyAppt') }}') }}.NEW_STUS_TRAK_I AND {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_ORIG_C = {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_ORIG_C AND {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_ORIG_SYST_C = {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_ORIG_SYST_C AND {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_RECV_S = {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_RECV_S AND {{ ref('{{ ref('CpyAppt') }}') }}.NEW_REL_MGR_STAT_C = {{ ref('{{ ref('CpyAppt') }}') }}.NEW_REL_MGR_STAT_C AND {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_RECV_D = {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_RECV_D AND {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_RECV_T = {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_RECV_T THEN '0'
		END AS change_code 
	FROM {{ ref('{{ ref('CpyAppt') }}') }} 
	FULL OUTER JOIN {{ ref('{{ ref('CpyAppt') }}') }} 
	ON {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_I = {{ ref('{{ ref('CpyAppt') }}') }}.NEW_APPT_I
	WHERE change_code = '1' OR change_code = '3'
)

SELECT * FROM ChangeCapture