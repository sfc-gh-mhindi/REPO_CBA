{{ config(materialized='view', tags=['XfmAppt_Pdct_Dept']) }}

WITH Xfm_Nom AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToTrans_Nom.DEPT_I) Then 'Y' Else  If Trim(ToTrans_Nom.DEPT_I) = '' Then 'Y' Else 'N',
		IFF({{ ref('LookUp_Nom') }}.DEPT_I IS NULL, 'Y', IFF(TRIM({{ ref('LookUp_Nom') }}.DEPT_I) = '', 'Y', 'N')) AS svIsNullDeptIN,
		-- *SRC*: 'CSEPO' : TRIM(ToTrans_Nom.APP_PROD_ID),
		CONCAT('CSEPO', TRIM({{ ref('LookUp_Nom') }}.APP_PROD_ID)) AS APPT_PDCT_I,
		-- *SRC*: \(20)If svIsNullDeptIN = 'N' Then trim(ToTrans_Nom.DEPT_I) Else 'Unknown',
		IFF(svIsNullDeptIN = 'N', TRIM({{ ref('LookUp_Nom') }}.DEPT_I), 'Unknown') AS DEPT_I,
		'NOMN' AS DEPT_ROLE_C,
		'CSE' AS SRCE_SYST_C,
		-- *SRC*: TRIM(ToTrans_Nom.NOMINATED_BSB),
		TRIM({{ ref('LookUp_Nom') }}.NOMINATED_BSB) AS BRCH_N,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS EFFT_D,
		-- *SRC*: StringToDate('9999-12-31', "%yyyy-%mm-%dd"),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		pRUN_STRM_C AS RUN_STRM
	FROM {{ ref('LookUp_Nom') }}
	WHERE 
)

SELECT * FROM Xfm_Nom