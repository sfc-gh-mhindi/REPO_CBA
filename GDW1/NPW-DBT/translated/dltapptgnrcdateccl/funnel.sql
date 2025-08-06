{{ config(materialized='view', tags=['DltApptGnrcDateCCL']) }}

WITH Funnel AS (
	SELECT
		APPT_I as APPT_I,
		DATE_ROLE_C as DATE_ROLE_C,
		EFFT_D as EFFT_D,
		GNRC_ROLE_S as GNRC_ROLE_S,
		GNRC_ROLE_D as GNRC_ROLE_D,
		GNRC_ROLE_T as GNRC_ROLE_T,
		PROS_KEY_EFFT_I as PROS_KEY_EFFT_I,
		EROR_SEQN_I as EROR_SEQN_I,
		MODF_S as MODF_S,
		MODF_S_TEMP as MODF_S_TEMP,
		MODF_D as MODF_D,
		MODF_T as MODF_T,
		USER_I as USER_I,
		CHNG_REAS_TYPE_C as CHNG_REAS_TYPE_C,
		change_code as change_code
	FROM {{ ref('Join') }}
	UNION ALL
	SELECT
		APPT_I,
		DATE_ROLE_C,
		EFFT_D,
		GNRC_ROLE_S,
		GNRC_ROLE_D,
		GNRC_ROLE_T,
		PROS_KEY_EFFT_I,
		EROR_SEQN_I,
		MODF_S,
		MODF_S_TEMP,
		MODF_D,
		MODF_T,
		USER_I,
		CHNG_REAS_TYPE_C,
		change_code
	FROM {{ ref('Column_Generator') }}
)

SELECT * FROM Funnel