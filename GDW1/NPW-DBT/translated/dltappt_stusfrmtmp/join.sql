{{ config(materialized='view', tags=['DltAppt_StusFrmTMP']) }}

WITH Join AS (
	SELECT
		{{ ref('src_cpy') }}.APPT_I,
		{{ ref('ChangeCapture') }}.STUS_C,
		{{ ref('ChangeCapture') }}.STRT_S,
		{{ ref('ChangeCapture') }}.STRT_D,
		{{ ref('ChangeCapture') }}.STRT_T,
		{{ ref('ChangeCapture') }}.END_D,
		{{ ref('ChangeCapture') }}.END_T,
		{{ ref('ChangeCapture') }}.END_S,
		{{ ref('ChangeCapture') }}.EMPL_I,
		{{ ref('src_cpy') }}.EFFT_D,
		{{ ref('src_cpy') }}.EXPY_D,
		{{ ref('src_cpy') }}.PROS_KEY_EFFT_I,
		{{ ref('src_cpy') }}.PROS_KEY_EXPY_I,
		{{ ref('src_cpy') }}.EROR_SEQN_I,
		{{ ref('ChangeCapture') }}.change_code
	FROM {{ ref('src_cpy') }}
	LEFT JOIN {{ ref('ChangeCapture') }} ON {{ ref('src_cpy') }}.APPT_I = {{ ref('ChangeCapture') }}.APPT_I
)

SELECT * FROM Join