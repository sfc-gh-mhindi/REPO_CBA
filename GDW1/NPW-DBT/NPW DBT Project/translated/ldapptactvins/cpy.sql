{{ config(materialized='view', tags=['LdApptActvIns']) }}

WITH Cpy AS (
	SELECT
		APPT_I,
		APPT_ACTV_TYPE_C,
		SRCE_SYST_C,
		APPT_ACTV_Q,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C
	FROM {{ ref('TgtApptActvDS') }}
)

SELECT * FROM Cpy