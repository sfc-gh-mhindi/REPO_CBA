{{ config(materialized='view', tags=['XfmEvntRel']) }}

WITH XfmTrans__Reject_Rec AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToTrans.OL_CLIENT_RM_RATING_ID) Then 'Y' Else  If Trim(ToTrans.OL_CLIENT_RM_RATING_ID) = '' Then 'Y' Else 'N',
		IFF({{ ref('LookUp') }}.OL_CLIENT_RM_RATING_ID IS NULL, 'Y', IFF(TRIM({{ ref('LookUp') }}.OL_CLIENT_RM_RATING_ID) = '', 'Y', 'N')) AS svIsNullEvntI,
		-- *SRC*: \(20)If IsNull(ToTrans.EVNT_I) Then 'Y' Else  If Trim(ToTrans.EVNT_I) = '' Then 'Y' Else 'N',
		IFF({{ ref('LookUp') }}.EVNT_I IS NULL, 'Y', IFF(TRIM({{ ref('LookUp') }}.EVNT_I) = '', 'Y', 'N')) AS svIsNullReldEvntI,
		MOD_TIMESTAMP,
		OL_CLIENT_RM_RATING_ID,
		CLIENT_ID,
		CIF_CODE,
		OU_ID,
		CS_USER_ID,
		RATING,
		WIM_PROCESS_ID,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS ETL_DATE,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS ORG_ETL_D,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((ToTrans.EVNT_I)) THEN (ToTrans.EVNT_I) ELSE ""))) = 0) Then 'REJ3000' Else '',
		IFF(LEN(TRIM(IFF({{ ref('LookUp') }}.EVNT_I IS NOT NULL, {{ ref('LookUp') }}.EVNT_I, ''))) = 0, 'REJ3000', '') AS EROR_C
	FROM {{ ref('LookUp') }}
	WHERE svIsNullReldEvntI = 'Y'
)

SELECT * FROM XfmTrans__Reject_Rec