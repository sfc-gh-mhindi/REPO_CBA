{{ config(materialized='view', tags=['DltAPPT_PRMO_TRAKFrmTMP_APPT_PRMO_TRAK']) }}

WITH xf_DecideRecordType__Ln_Update_Records AS (
	SELECT
		APPT_PDCT_I,
		TRAK_I,
		EFFT_D,
		-- *SRC*: DateFromDaysSince(-1, StringToDate(pRUN_STRM_PROS_D, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(pRUN_STRM_PROS_D, '%yyyy%mm%dd')) AS EXPY_D,
		pGDW_PROS_ID AS PROS_KEY_EXPY_I
	FROM {{ ref('tc_ApptPrmoTrakDlta') }}
	WHERE {{ ref('tc_ApptPrmoTrakDlta') }}.RECD_IND = 'D'
)

SELECT * FROM xf_DecideRecordType__Ln_Update_Records