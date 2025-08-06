{{ config(materialized='view', tags=['DltAPPT_STUSFrmTMP_APPT_STUS_CatchUp']) }}

WITH JoinAll AS (
	SELECT
		{{ ref('ChangeCapture') }}.NEW_APPT_I,
		{{ ref('ChangeCapture') }}.NEW_STUS_C,
		{{ ref('ChangeCapture') }}.NEW_STRT_S,
		{{ ref('ChangeCapture') }}.NEW_END_D,
		{{ ref('ChangeCapture') }}.NEW_END_T,
		{{ ref('ChangeCapture') }}.NEW_END_S,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CpyApptStus') }}.NEW_STRT_D,
		{{ ref('CpyApptStus') }}.NEW_STRT_T,
		{{ ref('CpyApptStus') }}.NEW_EMPL_I,
		{{ ref('CpyApptStus') }}.OLD_EFFT_D
	FROM {{ ref('ChangeCapture') }}
	INNER JOIN {{ ref('CpyApptStus') }} ON {{ ref('ChangeCapture') }}.NEW_APPT_I = {{ ref('CpyApptStus') }}.NEW_APPT_I
	AND {{ ref('ChangeCapture') }}.NEW_STRT_S = {{ ref('CpyApptStus') }}.NEW_STRT_S
	AND {{ ref('ChangeCapture') }}.NEW_STUS_C = {{ ref('CpyApptStus') }}.NEW_STUS_C
)

SELECT * FROM JoinAll