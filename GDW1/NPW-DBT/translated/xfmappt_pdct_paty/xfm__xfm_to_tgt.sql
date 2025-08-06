{{ config(materialized='view', tags=['XfmAppt_Pdct_Paty']) }}

WITH Xfm__Xfm_to_Tgt AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToTrans.APPT_QLFY_C) Then 'Y' Else  If Trim(ToTrans.APPT_QLFY_C) = '' Then 'Y' Else 'N',
		IFF({{ ref('FunlToXfm') }}.APPT_QLFY_C IS NULL, 'Y', IFF(TRIM({{ ref('FunlToXfm') }}.APPT_QLFY_C) = '', 'Y', 'N')) AS svIsNullApptQlfyC,
		-- *SRC*: \(20)If IsNull(ToTrans.PATY_ROLE_C) Then 'Y' Else  If Trim(ToTrans.PATY_ROLE_C) = '' Then 'Y' Else 'N',
		IFF({{ ref('FunlToXfm') }}.PATY_ROLE_C IS NULL, 'Y', IFF(TRIM({{ ref('FunlToXfm') }}.PATY_ROLE_C) = '', 'Y', 'N')) AS svIsNullPatyRoleC,
		-- *SRC*: \(20)If IsNull(ToTrans.ROLE_CAT_ID) Then 'Y' Else  If Trim(ToTrans.ROLE_CAT_ID) = '' Then 'Y' Else 'N',
		IFF({{ ref('FunlToXfm') }}.ROLE_CAT_ID IS NULL, 'Y', IFF(TRIM({{ ref('FunlToXfm') }}.ROLE_CAT_ID) = '', 'Y', 'N')) AS svIsNullRoleCatId,
		-- *SRC*: "CSE" : ( If svIsNullApptQlfyC = 'Y' Then "9999" Else ToTrans.APPT_QLFY_C) : ToTrans.APP_PROD_ID,
		CONCAT(CONCAT('CSE', IFF(svIsNullApptQlfyC = 'Y', '9999', {{ ref('FunlToXfm') }}.APPT_QLFY_C)), {{ ref('FunlToXfm') }}.APP_PROD_ID) AS APPT_PDCT_I,
		-- *SRC*: 'CIFPT+' : Right('0000000000' : ToTrans.CIF_CODE, 10),
		CONCAT('CIFPT+', RIGHT(CONCAT('0000000000', {{ ref('FunlToXfm') }}.CIF_CODE), 10)) AS PATY_I,
		-- *SRC*: \(20)If svIsNullRoleCatId = 'Y' Then 'UNKN' Else  if svIsNullPatyRoleC = 'Y' Then '9999' Else ToTrans.PATY_ROLE_C,
		IFF(svIsNullRoleCatId = 'Y', 'UNKN', IFF(svIsNullPatyRoleC = 'Y', '9999', {{ ref('FunlToXfm') }}.PATY_ROLE_C)) AS PATY_ROLE_C,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS EFFT_D,
		'CSE' AS SRCE_SYST_C,
		{{ ref('FunlToXfm') }}.APP_PROD_CLIENT_ROLE_ID AS SRCE_SYST_APPT_PDCT_PATY_I,
		-- *SRC*: StringToDate('9999-12-31', "%yyyy-%mm-%dd"),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		pRUN_STRM_C AS RUN_STRM
	FROM {{ ref('FunlToXfm') }}
	WHERE 
)

SELECT * FROM Xfm__Xfm_to_Tgt