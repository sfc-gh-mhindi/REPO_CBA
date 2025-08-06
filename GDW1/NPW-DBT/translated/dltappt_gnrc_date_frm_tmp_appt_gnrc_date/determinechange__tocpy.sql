{{ config(materialized='view', tags=['DltAPPT_GNRC_DATE_Frm_TMP_APPT_GNRC_DATE']) }}

WITH DetermineChange__ToCpy AS (
	SELECT
		APPT_I,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS EXPY_D,
		REFR_KEY AS PROS_KEY_EXPY_I
	FROM {{ ref('Join') }}
	WHERE {{ ref('Join') }}.change_code = 3
)

SELECT * FROM DetermineChange__ToCpy