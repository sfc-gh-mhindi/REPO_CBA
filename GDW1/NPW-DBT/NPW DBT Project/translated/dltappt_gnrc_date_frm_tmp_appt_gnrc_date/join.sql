{{ config(materialized='view', tags=['DltAPPT_GNRC_DATE_Frm_TMP_APPT_GNRC_DATE']) }}

WITH Join AS (
	SELECT
		{{ ref('ChangeCapture') }}.APPT_I,
		{{ ref('ChangeCapture') }}.GNRC_ROLE_D,
		{{ ref('ChangeCapture') }}.change_code,
		{{ ref('CopyFil') }}.DATE_ROLE_C,
		{{ ref('CopyFil') }}.EFFT_D,
		{{ ref('CopyFil') }}.GNRC_ROLE_S,
		{{ ref('CopyFil') }}.GNRC_ROLE_T,
		{{ ref('CopyFil') }}.PROS_KEY_EFFT_I,
		{{ ref('CopyFil') }}.PROS_KEY_EXPY_I,
		{{ ref('CopyFil') }}.EROR_SEQN_I,
		{{ ref('CopyFil') }}.MODF_S,
		{{ ref('CopyFil') }}.MODF_D,
		{{ ref('CopyFil') }}.MODF_T,
		{{ ref('CopyFil') }}.USER_I,
		{{ ref('CopyFil') }}.CHNG_REAS_TYPE_C,
		{{ ref('CopyFil') }}.EXPY_D
	FROM {{ ref('ChangeCapture') }}
	LEFT JOIN {{ ref('CopyFil') }} ON {{ ref('ChangeCapture') }}.APPT_I = {{ ref('CopyFil') }}.APPT_I
)

SELECT * FROM Join