{{ config(materialized='view', tags=['DltAPPT_GNRC_DATE_FrmTMP_APPT_GNRC_DATE']) }}

WITH DetermineChange__ToInserts AS (
	SELECT
		APPT_I,
		DATE_ROLE_C,
		EFFT_D,
		GNRC_ROLE_S,
		GNRC_ROLE_D,
		GNRC_ROLE_T,
		EXPY_D,
		REFR_KEY AS PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		MODF_S,
		MODF_D,
		MODF_T,
		USER_I,
		CHNG_REAS_TYPE_C
	FROM {{ ref('Join') }}
	WHERE {{ ref('Join') }}.change_code = 1 OR {{ ref('Join') }}.change_code = 3
)

SELECT * FROM DetermineChange__ToInserts