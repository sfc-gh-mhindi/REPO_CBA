{{ config(materialized='view', tags=['DltAPPT_ACTVFrmTMP_APPT_ACTV']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_APPT_I,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CpyApptActv') }}.NEW_APPT_ACTV_Q,
		{{ ref('CpyApptActv') }}.OLD_EFFT_D
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyApptActv') }} ON {{ ref('ChangeCapture') }}.NEW_APPT_I = {{ ref('CpyApptActv') }}.NEW_APPT_I
)

SELECT * FROM JoinAll