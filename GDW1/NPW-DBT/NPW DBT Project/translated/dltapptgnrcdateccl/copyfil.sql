{{ config(materialized='view', tags=['DltApptGnrcDateCCL']) }}

WITH CopyFil AS (
	SELECT
		APPT_I,
		DATE_ROLE_C,
		GNRC_ROLE_S,
		MODF_S,
		USER_I,
		CHNG_REAS_TYPE_C,
		EFFT_D,
		GNRC_ROLE_D,
		GNRC_ROLE_T,
		PROS_KEY_EFFT_I,
		EROR_SEQN_I,
		MODF_D,
		MODF_S_TEMP,
		MODF_T
	FROM {{ ref('Filter') }}
	WHERE MODF_S = MODF_S_TEMP
)

SELECT * FROM CopyFil