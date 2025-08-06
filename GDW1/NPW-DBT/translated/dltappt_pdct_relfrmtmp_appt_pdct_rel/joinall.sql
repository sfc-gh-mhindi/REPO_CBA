{{ config(materialized='view', tags=['DltAPPT_PDCT_RELFrmTMP_APPT_PDCT_REL']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_APPT_PDCT_I,
		{{ ref('ChangeCapture') }}.NEW_RELD_APPT_PDCT_I,
		{{ ref('ChangeCapture') }}.NEW_REL_TYPE_C,
		{{ ref('CpyApptPdctRel') }}.OLD_EFFT_D,
		{{ ref('ChangeCapture') }}.change_code
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyApptPdctRel') }} ON {{ ref('ChangeCapture') }}.NEW_APPT_PDCT_I = {{ ref('CpyApptPdctRel') }}.NEW_APPT_PDCT_I
	AND {{ ref('ChangeCapture') }}.NEW_RELD_APPT_PDCT_I = {{ ref('CpyApptPdctRel') }}.NEW_RELD_APPT_PDCT_I
)

SELECT * FROM JoinAll