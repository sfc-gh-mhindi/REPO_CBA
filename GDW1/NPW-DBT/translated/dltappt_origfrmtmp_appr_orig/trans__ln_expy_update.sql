{{ config(materialized='view', tags=['DltAPPT_ORIGFrmTMP_APPR_ORIG']) }}

WITH Trans__ln_expy_update AS (
	SELECT
		-- *SRC*: StringToDate((pRUN_STRM_PROS_D[1, 4] : '-' : pRUN_STRM_PROS_D[5, 2] : '-' : pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(SUBSTRING(pRUN_STRM_PROS_D, 1, 4), '-'), SUBSTRING(pRUN_STRM_PROS_D, 5, 2)), '-'), SUBSTRING(pRUN_STRM_PROS_D, 7, 2)), '%yyyy-%mm-%dd') AS svEtlDate,
		APPT_I,
		APPT_ORIG_CATG_C,
		EFFT_D,
		-- *SRC*: \(20)If ApptOrigOut.EFFT_D < svEtlDate Then (DateFromDaysSince(-1, svEtlDate)) Else svEtlDate,
		IFF({{ ref('tc_ApptOrig') }}.EFFT_D < svEtlDate, DATEFROMDAYSSINCE(-1, svEtlDate), svEtlDate) AS EXPY_D,
		{{ ref('tc_ApptOrig') }}.PROS_KEY_EFFT_I AS PROS_KEY_EXPY_I
	FROM {{ ref('tc_ApptOrig') }}
	WHERE {{ ref('tc_ApptOrig') }}.IND_FLAG = 'U'
)

SELECT * FROM Trans__ln_expy_update