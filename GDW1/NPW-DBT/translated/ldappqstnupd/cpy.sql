{{ config(materialized='view', tags=['LdAppQstnUpd']) }}

WITH CPY AS (
	SELECT
		APPT_I,
		PATY_I,
		QSTN_C,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM {{ ref('TgtApptUpdateDS') }}
)

SELECT * FROM CPY