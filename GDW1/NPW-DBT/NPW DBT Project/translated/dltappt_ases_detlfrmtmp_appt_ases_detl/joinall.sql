{{ config(materialized='view', tags=['DltAPPT_ASES_DETLFrmTMP_APPT_ASES_DETL']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_APPT_I,
		{{ ref('ChangeCapture') }}.NEW_AMT_TYPE_C,
		{{ ref('ChangeCapture') }}.NEW_APPT_ASES_A,
		{{ ref('CpyApptAsesDetl') }}.NEW_CNCY_C,
		{{ ref('CpyApptAsesDetl') }}.OLD_EFFT_D,
		{{ ref('ChangeCapture') }}.change_code
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyApptAsesDetl') }} ON {{ ref('ChangeCapture') }}.NEW_APPT_I = {{ ref('CpyApptAsesDetl') }}.NEW_APPT_I
	AND {{ ref('ChangeCapture') }}.NEW_AMT_TYPE_C = {{ ref('CpyApptAsesDetl') }}.NEW_AMT_TYPE_C
)

SELECT * FROM JoinAll