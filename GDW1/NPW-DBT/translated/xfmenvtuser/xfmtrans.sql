{{ config(materialized='view', tags=['XfmEnvtUser']) }}

WITH XfmTrans AS (
	SELECT
		-- *SRC*: 'CSE' : 'A7' : FrmSrc.OL_CLIENT_RM_RATING_ID,
		CONCAT(CONCAT('CSE', 'A7'), {{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}.OL_CLIENT_RM_RATING_ID) AS svEvntI,
		svEvntI AS EVNT_I,
		-- *SRC*: 'CSE' : 'C1' : FrmSrc.CS_USER_ID,
		CONCAT(CONCAT('CSE', 'C1'), {{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}.CS_USER_ID) AS USER_I,
		'RMRC' AS EVNT_PATY_ROLE_C,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS EFFT_D,
		'9999-12-31' AS EXPY_D,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}
	WHERE 
)

SELECT * FROM XfmTrans