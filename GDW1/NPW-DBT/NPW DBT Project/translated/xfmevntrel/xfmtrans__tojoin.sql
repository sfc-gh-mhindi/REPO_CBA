{{ config(materialized='view', tags=['XfmEvntRel']) }}

WITH XfmTrans__ToJoin AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToTrans.OL_CLIENT_RM_RATING_ID) Then 'Y' Else  If Trim(ToTrans.OL_CLIENT_RM_RATING_ID) = '' Then 'Y' Else 'N',
		IFF({{ ref('LookUp') }}.OL_CLIENT_RM_RATING_ID IS NULL, 'Y', IFF(TRIM({{ ref('LookUp') }}.OL_CLIENT_RM_RATING_ID) = '', 'Y', 'N')) AS svIsNullEvntI,
		-- *SRC*: \(20)If IsNull(ToTrans.EVNT_I) Then 'Y' Else  If Trim(ToTrans.EVNT_I) = '' Then 'Y' Else 'N',
		IFF({{ ref('LookUp') }}.EVNT_I IS NULL, 'Y', IFF(TRIM({{ ref('LookUp') }}.EVNT_I) = '', 'Y', 'N')) AS svIsNullReldEvntI,
		-- *SRC*: 'CSE' : 'A7' : Trim(ToTrans.OL_CLIENT_RM_RATING_ID),
		CONCAT(CONCAT('CSE', 'A7'), TRIM({{ ref('LookUp') }}.OL_CLIENT_RM_RATING_ID)) AS EVNT_I,
		{{ ref('LookUp') }}.EVNT_I AS RELD_EVNT_I,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS EFFT_D,
		'RMRW' AS EVNT_REL_TYPE_C,
		'9999-12-31' AS EXPY_D,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		'0' AS ROW_SECU_ACCS_C
	FROM {{ ref('LookUp') }}
	WHERE svIsNullEvntI = 'N' AND svIsNullReldEvntI = 'N'
)

SELECT * FROM XfmTrans__ToJoin