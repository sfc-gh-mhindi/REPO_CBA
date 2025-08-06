{{ config(materialized='view', tags=['DltApptGnrcDateCCL']) }}

WITH Join AS (
	SELECT
		{{ ref('ChangeCapture') }}.APPT_I,
		{{ ref('ChangeCapture') }}.DATE_ROLE_C,
		{{ ref('CopyFil') }}.EFFT_D,
		{{ ref('ChangeCapture') }}.GNRC_ROLE_S,
		{{ ref('CopyFil') }}.GNRC_ROLE_D,
		{{ ref('CopyFil') }}.GNRC_ROLE_T,
		{{ ref('CopyFil') }}.PROS_KEY_EFFT_I,
		{{ ref('CopyFil') }}.EROR_SEQN_I,
		{{ ref('ChangeCapture') }}.MODF_S,
		{{ ref('CopyFil') }}.MODF_S_TEMP,
		{{ ref('CopyFil') }}.MODF_D,
		{{ ref('CopyFil') }}.MODF_T,
		{{ ref('ChangeCapture') }}.USER_I,
		{{ ref('ChangeCapture') }}.CHNG_REAS_TYPE_C,
		{{ ref('ChangeCapture') }}.change_code
	FROM {{ ref('ChangeCapture') }}
	LEFT JOIN {{ ref('CopyFil') }} ON {{ ref('ChangeCapture') }}.APPT_I = {{ ref('CopyFil') }}.APPT_I
	AND {{ ref('ChangeCapture') }}.DATE_ROLE_C = {{ ref('CopyFil') }}.DATE_ROLE_C
)

SELECT * FROM Join