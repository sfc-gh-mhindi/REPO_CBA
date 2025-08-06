{{ config(materialized='view', tags=['LdApptDocuDelyInssUpd']) }}

WITH Cpy AS (
	SELECT
		APPT_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM {{ ref('TgtApptDocuDelyInssUpdateDS') }}
)

SELECT * FROM Cpy