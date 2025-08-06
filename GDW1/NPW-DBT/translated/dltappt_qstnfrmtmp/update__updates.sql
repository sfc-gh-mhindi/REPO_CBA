{{ config(materialized='view', tags=['DltAppt_QstnFrmTMP']) }}

WITH Update__updates AS (
	SELECT
		APPT_I,
		QSTN_C,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(pRUN_STRM_PROS_D, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(pRUN_STRM_PROS_D, '%yyyy%mm%dd')) AS EXPY_D,
		pGDW_PROS_ID AS PROS_KEY_EXPY_I
	FROM {{ ref('FrmCpyandCCpture') }}
	WHERE {{ ref('FrmCpyandCCpture') }}.change_code = 3
)

SELECT * FROM Update__updates