{{ config(materialized='view', tags=['DltAPPT_GNRC_DATE_FrmTMP_APPT_GNRC_DATE']) }}

WITH CopyFil AS (
	SELECT
		APPT_I,
		GNRC_ROLE_D,
		DATE_ROLE_C,
		EFFT_D,
		GNRC_ROLE_S,
		GNRC_ROLE_T,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		MODF_S,
		MODF_D,
		MODF_T,
		USER_I,
		CHNG_REAS_TYPE_C,
		EXPY_D
	FROM {{ ref('TrfrmrtoCpy') }}
)

SELECT * FROM CopyFil