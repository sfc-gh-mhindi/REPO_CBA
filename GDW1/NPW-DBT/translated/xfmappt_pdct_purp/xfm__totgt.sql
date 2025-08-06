{{ config(materialized='view', tags=['XfmAppt_Pdct_Purp']) }}

WITH Xfm__ToTgt AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToLkp.PURP_TYPE_C) Then 'Y' Else  if Trim(ToLkp.PURP_TYPE_C) = '' THEN 'Y' ELSE 'N',
		IFF({{ ref('LkpPurpTypeC') }}.PURP_TYPE_C IS NULL, 'Y', IFF(TRIM({{ ref('LkpPurpTypeC') }}.PURP_TYPE_C) = '', 'Y', 'N')) AS svIsNullPurpTypeC,
		-- *SRC*: 'CSEPO' : ToLkp.APP_PROD_ID,
		CONCAT('CSEPO', {{ ref('LkpPurpTypeC') }}.APP_PROD_ID) AS APPT_PDCT_I,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS EFFT_D,
		{{ ref('LkpPurpTypeC') }}.PL_PROD_PURP_CAT_ID AS SRCE_SYST_APPT_PDCT_PURP_I,
		-- *SRC*: \(20)if svIsNullPurpTypeC = 'Y' Then '9999' Else ToLkp.PURP_TYPE_C,
		IFF(svIsNullPurpTypeC = 'Y', '9999', {{ ref('LkpPurpTypeC') }}.PURP_TYPE_C) AS PURP_TYPE_C,
		-- *SRC*: SetNull(),
		SETNULL() AS PURP_CLAS_C,
		'CSE' AS SRCE_SYST_C,
		0 AS PURP_A,
		-- *SRC*: SetNull(),
		SETNULL() AS CNCY_C,
		-- *SRC*: SetNull(),
		SETNULL() AS MAIN_PURP_F,
		-- *SRC*: StringToDate('9999-12-31', "%yyyy-%mm-%dd"),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		pRUN_STRM_C AS RUN_STRM
	FROM {{ ref('LkpPurpTypeC') }}
	WHERE 
)

SELECT * FROM Xfm__ToTgt