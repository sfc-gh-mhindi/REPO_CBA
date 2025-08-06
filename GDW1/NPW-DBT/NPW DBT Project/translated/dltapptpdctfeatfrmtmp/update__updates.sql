{{ config(materialized='view', tags=['DltApptPdctFeatFrmTMP']) }}

WITH Update__updates AS (
	SELECT
		APPT_PDCT_I,
		FEAT_I,
		SRCE_SYST_C,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('Join') }}
	WHERE {{ ref('Join') }}.change_code = 3
)

SELECT * FROM Update__updates