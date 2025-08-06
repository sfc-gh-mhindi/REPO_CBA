{{ config(materialized='view', tags=['LdApptActvUpd']) }}

WITH Cpy AS (
	SELECT
		APPT_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM {{ ref('TgtApptActvUpdateDS') }}
)

SELECT * FROM Cpy