{{ config(materialized='view', tags=['XfmAppt_Pdct_Purp']) }}

WITH Xfm__ToErr AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToLkp.PURP_TYPE_C) Then 'Y' Else  if Trim(ToLkp.PURP_TYPE_C) = '' THEN 'Y' ELSE 'N',
		IFF({{ ref('LkpPurpTypeC') }}.PURP_TYPE_C IS NULL, 'Y', IFF(TRIM({{ ref('LkpPurpTypeC') }}.PURP_TYPE_C) = '', 'Y', 'N')) AS svIsNullPurpTypeC,
		-- *SRC*: TRIM(ToLkp.APP_PROD_ID),
		TRIM({{ ref('LkpPurpTypeC') }}.APP_PROD_ID) AS SRCE_KEY_I,
		pGDW_LOAD_USER AS CONV_M,
		'LOOKUP FAILURE ON MAP_CSE_APPT_PDCT_PURP_PO' AS CONV_MAP_RULE_M,
		'APPT_PDCT_PURP' AS TRSF_TABL_M,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS SRCE_EFFT_D,
		-- *SRC*: TRIM(ToLkp.PL_PROD_PURP_CAT_ID),
		TRIM({{ ref('LkpPurpTypeC') }}.PL_PROD_PURP_CAT_ID) AS VALU_CHNG_BFOR_X,
		'9999' AS VALU_CHNG_AFTR_X,
		DSJobName AS TRSF_X,
		'PURP_TYPE_C' AS TRSF_COLM_M,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		'CSE_CPO_BUS_APP_PROD' AS SRCE_FILE_M,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		-- *SRC*: 'CSEPO' : ToLkp.APP_PROD_ID,
		CONCAT('CSEPO', {{ ref('LkpPurpTypeC') }}.APP_PROD_ID) AS TRSF_KEY_I
	FROM {{ ref('LkpPurpTypeC') }}
	WHERE svIsNullPurpTypeC = 'Y'
)

SELECT * FROM Xfm__ToErr