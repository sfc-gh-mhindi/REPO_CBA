{{ config(materialized='view', tags=['DltAppt_Pdct_AmtFrmTMP']) }}

WITH Update__updates AS (
	SELECT
		APPT_PDCT_I,
		AMT_TYPE_C,
		SRCE_SYST_C,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(pRUN_STRM_PROS_D, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(pRUN_STRM_PROS_D, '%yyyy%mm%dd')) AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('Join') }}
	WHERE {{ ref('Join') }}.change_code = 3
)

SELECT * FROM Update__updates