{{ config(materialized='view', tags=['LdApptDocuIns']) }}

WITH Cpy AS (
	SELECT
		APPT_I,
		DOCU_C,
		SRCE_SYST_C,
		DOCU_VERS_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C
	FROM {{ ref('Appt_Docu_DS') }}
)

SELECT * FROM Cpy