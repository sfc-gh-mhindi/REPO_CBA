{{ config(materialized='view', tags=['DltAPPTFrmTMP_APPT']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_APPT_I,
		{{ ref('ChangeCapture') }}.NEW_APPT_C,
		{{ ref('ChangeCapture') }}.NEW_APPT_FORM_C,
		{{ ref('ChangeCapture') }}.NEW_STUS_TRAK_I,
		{{ ref('ChangeCapture') }}.NEW_APPT_ORIG_C,
		{{ ref('CpyAppt') }}.NEW_RATE_SEEK_F,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CpyAppt') }}.NEW_APPT_QLFY_C,
		{{ ref('CpyAppt') }}.NEW_APPT_N,
		{{ ref('CpyAppt') }}.NEW_SRCE_SYST_C,
		{{ ref('CpyAppt') }}.NEW_SRCE_SYST_APPT_I,
		{{ ref('CpyAppt') }}.NEW_APPT_CRAT_D
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyAppt') }} ON {{ ref('ChangeCapture') }}.NEW_APPT_I = {{ ref('CpyAppt') }}.NEW_APPT_I
)

SELECT * FROM JoinAll