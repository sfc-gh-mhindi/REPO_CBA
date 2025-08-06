{{ config(materialized='view', tags=['XfmPatyEvnt']) }}

WITH XfmTrans AS (
	SELECT
		-- *SRC*: \(20)IF IsNull(FrmSrc.CIF_CODE) THEN "Y" ELSE  IF FrmSrc.CIF_CODE = ' ' THEN "Y" ELSE "N",
		IFF({{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}.CIF_CODE IS NULL, 'Y', IFF({{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}.CIF_CODE = ' ', 'Y', 'N')) AS svIsNullCifCode,
		-- *SRC*: 'CSE' : 'A7' : FrmSrc.OL_CLIENT_RM_RATING_ID,
		CONCAT(CONCAT('CSE', 'A7'), {{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}.OL_CLIENT_RM_RATING_ID) AS svEvntI,
		svEvntI AS EVNT_I,
		{{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}.CIF_CODE AS SRCE_SYST_PATY_I,
		'RMRA' AS EVNT_PATY_ROLE_TYPE_C,
		'CSE' AS SRCE_SYST_C,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS EFFT_D,
		-- *SRC*: 'CIFPT+' : Right('0000000000' : FrmSrc.CIF_CODE, 10),
		CONCAT('CIFPT+', RIGHT(CONCAT('0000000000', {{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}.CIF_CODE), 10)) AS PATY_I,
		'9999-12-31' AS EXPY_D,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		0 AS ROW_SECU_ACCS_C
	FROM {{ ref('CSE_ONLN_BUS_OL_CLNT_RM_RATE') }}
	WHERE svIsNullCifCode = 'N'
)

SELECT * FROM XfmTrans