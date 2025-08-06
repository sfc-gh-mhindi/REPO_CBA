{{ config(materialized='view', tags=['XfmAppt_Pdct_Rpay']) }}

WITH Xfm__ToErr AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToXfm.RPAY_SRCE_C) Then 'Y' Else  If Trim(ToXfm.RPAY_SRCE_C) = '' Then 'Y' Else 'N',
		IFF({{ ref('Lkp') }}.RPAY_SRCE_C IS NULL, 'Y', IFF(TRIM({{ ref('Lkp') }}.RPAY_SRCE_C) = '', 'Y', 'N')) AS svIsNullRpaySrceC,
		-- *SRC*: \(20)If IsNull(ToXfm.PO_REPAYMENT_SOURCE_OTHER) Then 'Y' Else  if Trim(ToXfm.PO_REPAYMENT_SOURCE_OTHER) = '' Then 'Y' Else  if Trim(ToXfm.PO_REPAYMENT_SOURCE_OTHER) = 0 Then 'Y' Else 'N',
		IFF({{ ref('Lkp') }}.PO_REPAYMENT_SOURCE_OTHER IS NULL, 'Y', IFF(TRIM({{ ref('Lkp') }}.PO_REPAYMENT_SOURCE_OTHER) = '', 'Y', IFF(TRIM({{ ref('Lkp') }}.PO_REPAYMENT_SOURCE_OTHER) = 0, 'Y', 'N'))) AS svIsNullPoReypaymentSourceOther,
		{{ ref('Lkp') }}.APP_PROD_ID AS SRCE_KEY_I,
		pGDW_LOAD_USER AS CONV_M,
		'LOOKUP FAILURE ON MAP_CSE_APPT_PDCT_RPAY' AS CONV_MAP_RULE_M,
		'APPT_PDCT_RPAY' AS TRSF_TABL_M,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS SRCE_EFFT_D,
		{{ ref('Lkp') }}.PO_REPAYMENT_SOURCE_CAT_ID AS VALU_CHNG_BFOR_X,
		9999 AS VALU_CHNG_AFTR_X,
		DSJobName AS TRSF_X,
		'RPAY_SRCE_C' AS TRSF_COLM_M,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		'CSE_CPO_BUS_APP_PROD' AS SRCE_FILE_M,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		-- *SRC*: "CSEPO" : ToXfm.APP_PROD_ID,
		CONCAT('CSEPO', {{ ref('Lkp') }}.APP_PROD_ID) AS TRSF_KEY_I
	FROM {{ ref('Lkp') }}
	WHERE svIsNullRpaySrceC = 'Y'
)

SELECT * FROM Xfm__ToErr