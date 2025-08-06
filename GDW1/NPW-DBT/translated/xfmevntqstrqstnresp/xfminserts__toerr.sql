{{ config(materialized='view', tags=['XfmEvntQstrQstnResp']) }}

WITH XfmInserts__ToErr AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToEvntRel.RESP_C) Then 'Y' Else  If Trim(ToEvntRel.RESP_C) = '' Then 'Y' Else 'N',
		IFF({{ ref('LeftOuterJoin') }}.RESP_C IS NULL, 'Y', IFF(TRIM({{ ref('LeftOuterJoin') }}.RESP_C) = '', 'Y', 'N')) AS svIsNullRespC,
		{{ ref('LeftOuterJoin') }}.OL_CLIENT_RM_RATING_ID AS SRCE_KEY_I,
		pGDW_LOAD_USER AS CONV_M,
		'LOOKUP FAILURE ON MAP_CSE_QSTR_QSTN_RESP_RM' AS CONV_MAP_RULE_M,
		'EVNT_QSTR_QSTN_RESP' AS TRSF_TABL_M,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS SRCE_EFFT_D,
		{{ ref('LeftOuterJoin') }}.RATING AS VALU_CHNG_BFOR_X,
		'9999' AS VALU_CHNG_AFTR_X,
		DSJobName AS TRSF_X,
		'RESP_C' AS TRSF_COLM_M,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		'CSE_ONLN_BUS_OL_CLNT_RM_RATE' AS SRCE_FILE_M,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		-- *SRC*: 'CSE' : 'A7' : Trim(ToEvntRel.OL_CLIENT_RM_RATING_ID),
		CONCAT(CONCAT('CSE', 'A7'), TRIM({{ ref('LeftOuterJoin') }}.OL_CLIENT_RM_RATING_ID)) AS TRSF_KEY_I
	FROM {{ ref('LeftOuterJoin') }}
	WHERE svIsNullRespC = 'Y' AND {{ ref('LeftOuterJoin') }}.dummy = 0
)

SELECT * FROM XfmInserts__ToErr