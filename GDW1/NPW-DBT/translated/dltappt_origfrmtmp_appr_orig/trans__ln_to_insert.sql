{{ config(materialized='view', tags=['DltAPPT_ORIGFrmTMP_APPR_ORIG']) }}

WITH Trans__ln_to_insert AS (
	SELECT
		-- *SRC*: StringToDate((pRUN_STRM_PROS_D[1, 4] : '-' : pRUN_STRM_PROS_D[5, 2] : '-' : pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(SUBSTRING(pRUN_STRM_PROS_D, 1, 4), '-'), SUBSTRING(pRUN_STRM_PROS_D, 5, 2)), '-'), SUBSTRING(pRUN_STRM_PROS_D, 7, 2)), '%yyyy-%mm-%dd') AS svEtlDate,
		APPT_I,
		svEtlDate AS EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		ROW_SECU_ACCS_C,
		SRCE_SYST_C,
		APPT_ORIG_C,
		APPT_ORIG_CATG_C
	FROM {{ ref('tc_ApptOrig') }}
	WHERE {{ ref('tc_ApptOrig') }}.IND_FLAG = 'I' OR {{ ref('tc_ApptOrig') }}.IND_FLAG = 'U'
)

SELECT * FROM Trans__ln_to_insert