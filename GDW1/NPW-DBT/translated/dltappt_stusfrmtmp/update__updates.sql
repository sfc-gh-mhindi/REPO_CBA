{{ config(materialized='view', tags=['DltAppt_StusFrmTMP']) }}

WITH Update__updates AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I,
		APPT_I
	FROM {{ ref('FunlTransAndJoin') }}
	WHERE {{ ref('FunlTransAndJoin') }}.change_code = 3
)

SELECT * FROM Update__updates