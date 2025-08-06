{{ config(materialized='view', tags=['XfmEvntQstrQstnResp']) }}

WITH XfmTrans AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToTrans.OL_CLIENT_RM_RATING_ID) Then 'Y' Else  If Trim(ToTrans.OL_CLIENT_RM_RATING_ID) = '' Then 'Y' Else 'N',
		IFF({{ ref('LookUp') }}.OL_CLIENT_RM_RATING_ID IS NULL, 'Y', IFF(TRIM({{ ref('LookUp') }}.OL_CLIENT_RM_RATING_ID) = '', 'Y', 'N')) AS svIsNullEvntI,
		-- *SRC*: 'CSE' : 'A7' : Trim(ToTrans.OL_CLIENT_RM_RATING_ID),
		CONCAT(CONCAT('CSE', 'A7'), TRIM({{ ref('LookUp') }}.OL_CLIENT_RM_RATING_ID)) AS EVNT_I,
		'CRMR' AS QSTR_C,
		'RMRT' AS QSTN_C,
		RESP_C,
		-- *SRC*: SetNull(),
		SETNULL() AS RESP_VALU_N,
		-- *SRC*: SetNull(),
		SETNULL() AS RESP_VALU_S,
		-- *SRC*: SetNull(),
		SETNULL() AS RESP_VALU_D,
		-- *SRC*: SetNull(),
		SETNULL() AS RESP_VALU_T,
		-- *SRC*: SetNull(),
		SETNULL() AS RESP_VALU_X,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS EFFT_D,
		'9999-12-31' AS EXPY_D,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		'0' AS ROW_SECU_ACCS_C,
		OL_CLIENT_RM_RATING_ID,
		RATING
	FROM {{ ref('LookUp') }}
	WHERE svIsNullEvntI = 'N'
)

SELECT * FROM XfmTrans