{{ config(materialized='view', tags=['XfmEvntQstr']) }}

WITH XfmTrans AS (
	SELECT
		-- *SRC*: \(20)If IsNull(FrmSrc.OL_CLIENT_RM_RATING_ID) Then 'Y' Else  If Trim(FrmSrc.OL_CLIENT_RM_RATING_ID) = '' Then 'Y' Else 'N',
		IFF({{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}.OL_CLIENT_RM_RATING_ID IS NULL, 'Y', IFF(TRIM({{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}.OL_CLIENT_RM_RATING_ID) = '', 'Y', 'N')) AS svIsNullEvntI,
		-- *SRC*: 'CSE' : 'A7' : Trim(FrmSrc.OL_CLIENT_RM_RATING_ID),
		CONCAT(CONCAT('CSE', 'A7'), TRIM({{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}.OL_CLIENT_RM_RATING_ID)) AS EVNT_I,
		'CRMR' AS QSTR_C,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS EFFT_D,
		'9999-12-31' AS EXPY_D,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I
	FROM {{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}
	WHERE svIsNullEvntI = 'N'
)

SELECT * FROM XfmTrans