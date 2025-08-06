{{ config(materialized='view', tags=['DltAppt_QstnFrmTMP']) }}

WITH Update__Appt_Qstn_Ins AS (
	SELECT
		APPT_I,
		QSTN_C,
		RESP_C,
		RESP_CMMT_X,
		PATY_I,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS EFFT_D,
		-- *SRC*: StringToDate('9999-12-31', "%yyyy-%mm-%dd"),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		0 AS ROW_SECU_ACCS_C,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('FrmCpyandCCpture') }}
	WHERE {{ ref('FrmCpyandCCpture') }}.change_code = 1 OR {{ ref('FrmCpyandCCpture') }}.change_code = 3
)

SELECT * FROM Update__Appt_Qstn_Ins