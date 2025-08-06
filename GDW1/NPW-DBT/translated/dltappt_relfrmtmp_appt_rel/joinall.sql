{{ config(materialized='view', tags=['DltAPPT_RELFrmTMP_APPT_REL']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_APPT_I,
		{{ ref('ChangeCapture') }}.NEW_RELD_APPT_I,
		{{ ref('ChangeCapture') }}.NEW_REL_TYPE_C,
		{{ ref('CpyApptRel') }}.OLD_EFFT_D,
		{{ ref('ChangeCapture') }}.change_code
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyApptRel') }} ON {{ ref('ChangeCapture') }}.NEW_APPT_I = {{ ref('CpyApptRel') }}.NEW_APPT_I
	AND {{ ref('ChangeCapture') }}.NEW_RELD_APPT_I = {{ ref('CpyApptRel') }}.NEW_RELD_APPT_I
	AND {{ ref('ChangeCapture') }}.NEW_REL_TYPE_C = {{ ref('CpyApptRel') }}.NEW_REL_TYPE_C
)

SELECT * FROM JoinAll