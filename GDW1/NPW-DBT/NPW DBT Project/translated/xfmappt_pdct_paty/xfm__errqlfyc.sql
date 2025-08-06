{{ config(materialized='view', tags=['XfmAppt_Pdct_Paty']) }}

WITH Xfm__ErrQlfyC AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToTrans.APPT_QLFY_C) Then 'Y' Else  If Trim(ToTrans.APPT_QLFY_C) = '' Then 'Y' Else 'N',
		IFF({{ ref('FunlToXfm') }}.APPT_QLFY_C IS NULL, 'Y', IFF(TRIM({{ ref('FunlToXfm') }}.APPT_QLFY_C) = '', 'Y', 'N')) AS svIsNullApptQlfyC,
		-- *SRC*: \(20)If IsNull(ToTrans.PATY_ROLE_C) Then 'Y' Else  If Trim(ToTrans.PATY_ROLE_C) = '' Then 'Y' Else 'N',
		IFF({{ ref('FunlToXfm') }}.PATY_ROLE_C IS NULL, 'Y', IFF(TRIM({{ ref('FunlToXfm') }}.PATY_ROLE_C) = '', 'Y', 'N')) AS svIsNullPatyRoleC,
		-- *SRC*: \(20)If IsNull(ToTrans.ROLE_CAT_ID) Then 'Y' Else  If Trim(ToTrans.ROLE_CAT_ID) = '' Then 'Y' Else 'N',
		IFF({{ ref('FunlToXfm') }}.ROLE_CAT_ID IS NULL, 'Y', IFF(TRIM({{ ref('FunlToXfm') }}.ROLE_CAT_ID) = '', 'Y', 'N')) AS svIsNullRoleCatId,
		{{ ref('FunlToXfm') }}.APP_PROD_ID AS SRCE_KEY_I,
		pGDW_LOAD_USER AS CONV_M,
		'LOOKUP FAILURE ON MAP_CSE_APPT_QLFY' AS CONV_MAP_RULE_M,
		'APPT_PDCT_PATY' AS TRSF_TABL_M,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS SRCE_EFFT_D,
		{{ ref('FunlToXfm') }}.SUBTYPE_CODE AS VALU_CHNG_BFOR_X,
		'9999' AS VALU_CHNG_AFTR_X,
		DSJobName AS TRSF_X,
		'APPT_QLFY_C' AS TRSF_COLM_M,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		'CSE_COM_BUS_APP_PROD_CLIENT_ROLE' AS SRCE_FILE_M,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		-- *SRC*: "CSE" : ( If svIsNullApptQlfyC = 'Y' Then "9999" Else ToTrans.APPT_QLFY_C) : ToTrans.APP_PROD_ID,
		CONCAT(CONCAT('CSE', IFF(svIsNullApptQlfyC = 'Y', '9999', {{ ref('FunlToXfm') }}.APPT_QLFY_C)), {{ ref('FunlToXfm') }}.APP_PROD_ID) AS TRSF_KEY_I
	FROM {{ ref('FunlToXfm') }}
	WHERE svIsNullApptQlfyC = 'Y'
)

SELECT * FROM Xfm__ErrQlfyC