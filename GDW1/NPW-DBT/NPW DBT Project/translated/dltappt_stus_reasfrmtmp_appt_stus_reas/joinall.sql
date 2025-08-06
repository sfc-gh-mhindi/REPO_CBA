{{ config(materialized='view', tags=['DltAPPT_STUS_REASFrmTMP_APPT_STUS_REAS']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_APPT_I,
		{{ ref('ChangeCapture') }}.NEW_STUS_C,
		{{ ref('ChangeCapture') }}.NEW_STUS_REAS_TYPE_C,
		{{ ref('ChangeCapture') }}.NEW_STRT_S,
		{{ ref('ChangeCapture') }}.NEW_END_S,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CpyApptStusReas') }}.OLD_EFFT_D
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyApptStusReas') }} ON {{ ref('ChangeCapture') }}.NEW_APPT_I = {{ ref('CpyApptStusReas') }}.NEW_APPT_I
	AND {{ ref('ChangeCapture') }}.NEW_STUS_C = {{ ref('CpyApptStusReas') }}.NEW_STUS_C
	AND {{ ref('ChangeCapture') }}.NEW_STUS_REAS_TYPE_C = {{ ref('CpyApptStusReas') }}.NEW_STUS_REAS_TYPE_C
	AND {{ ref('ChangeCapture') }}.NEW_STRT_S = {{ ref('CpyApptStusReas') }}.NEW_STRT_S
)

SELECT * FROM JoinAll