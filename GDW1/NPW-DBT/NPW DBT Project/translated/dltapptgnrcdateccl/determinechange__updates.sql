{{ config(materialized='view', tags=['DltApptGnrcDateCCL']) }}

WITH DetermineChange__updates AS (
	SELECT
		APPT_I,
		DATE_ROLE_C,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(pRUN_STRM_PROS_D, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(pRUN_STRM_PROS_D, '%yyyy%mm%dd')) AS EXPY_D,
		pGDW_PROS_ID AS PROS_KEY_EXPY_I
	FROM {{ ref('Funnel') }}
	WHERE {{ ref('Funnel') }}.change_code = 3
)

SELECT * FROM DetermineChange__updates