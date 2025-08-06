{{ config(materialized='view', tags=['XfmAppt_Pdct']) }}

WITH Xfm__ToErrApptDurtId AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToXfm.PO_OVERDRAFT_CAT_ID) Then 'Y' Else  if Trim(ToXfm.PO_OVERDRAFT_CAT_ID) = '0' Then 'Y' Else  if ( IF IsNotNull((ToXfm.PO_OVERDRAFT_CAT_ID)) THEN (ToXfm.PO_OVERDRAFT_CAT_ID) ELSE "") = '' Then 'Y' Else 'N',
		IFF({{ ref('FnlToXfm') }}.PO_OVERDRAFT_CAT_ID IS NULL, 'Y', IFF(TRIM({{ ref('FnlToXfm') }}.PO_OVERDRAFT_CAT_ID) = '0', 'Y', IFF(IFF({{ ref('FnlToXfm') }}.PO_OVERDRAFT_CAT_ID IS NOT NULL, {{ ref('FnlToXfm') }}.PO_OVERDRAFT_CAT_ID, '') = '', 'Y', 'N'))) AS svIsNullPoOverdraftCatId,
		-- *SRC*: \(20)If IsNull(ToXfm.APPT_PDCT_CATG_C) OR Trim(( IF IsNotNull((ToXfm.APPT_PDCT_CATG_C)) THEN (ToXfm.APPT_PDCT_CATG_C) ELSE "")) = '' Then 'Y' ELSE 'N',
		IFF({{ ref('FnlToXfm') }}.APPT_PDCT_CATG_C IS NULL OR TRIM(IFF({{ ref('FnlToXfm') }}.APPT_PDCT_CATG_C IS NOT NULL, {{ ref('FnlToXfm') }}.APPT_PDCT_CATG_C, '')) = '', 'Y', 'N') AS svIsNullApptPdctCatgC,
		-- *SRC*: \(20)If IsNull(ToXfm.APPT_PDCT_DURT_C) OR Trim(( IF IsNotNull((ToXfm.APPT_PDCT_DURT_C)) THEN (ToXfm.APPT_PDCT_DURT_C) ELSE "")) = '' Then 'Y' ELSE 'N',
		IFF({{ ref('FnlToXfm') }}.APPT_PDCT_DURT_C IS NULL OR TRIM(IFF({{ ref('FnlToXfm') }}.APPT_PDCT_DURT_C IS NOT NULL, {{ ref('FnlToXfm') }}.APPT_PDCT_DURT_C, '')) = '', 'Y', 'N') AS svIsNullApptPdctDurtC,
		-- *SRC*: \(20)If IsNull(ToXfm.APP_PROD_ID) Then 'Y' Else  if Trim(ToXfm.APP_PROD_ID) = '0' Then 'Y' Else  if ( IF IsNotNull((ToXfm.APP_PROD_ID)) THEN (ToXfm.APP_PROD_ID) ELSE "") = '' Then 'Y' Else 'N',
		IFF({{ ref('FnlToXfm') }}.APP_PROD_ID IS NULL, 'Y', IFF(TRIM({{ ref('FnlToXfm') }}.APP_PROD_ID) = '0', 'Y', IFF(IFF({{ ref('FnlToXfm') }}.APP_PROD_ID IS NOT NULL, {{ ref('FnlToXfm') }}.APP_PROD_ID, '') = '', 'Y', 'N'))) AS svIsNullAppProdId,
		{{ ref('FnlToXfm') }}.APP_PROD_ID AS SRCE_KEY_I,
		pGDW_LOAD_USER AS CONV_M,
		'LOOK UP FAILURE APPT_PDCT_DURT_C' AS CONV_MAP_RULE_M,
		'APPT_PDCT' AS TRSF_TABL_M,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS SRCE_EFFT_D,
		{{ ref('FnlToXfm') }}.PO_OVERDRAFT_CAT_ID AS VALU_CHNG_BFOR_X,
		'9999' AS VALU_CHNG_AFTR_X,
		DSJobName AS TRSF_X,
		'APPT_PDCT_DURT_C' AS TRSF_COLM_M,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		'CSE_CPO_BUS_APP_PROD' AS SRCE_FILE_M,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		-- *SRC*: 'CSEPO' : ToXfm.APP_PROD_ID,
		CONCAT('CSEPO', {{ ref('FnlToXfm') }}.APP_PROD_ID) AS TRSF_KEY_I
	FROM {{ ref('FnlToXfm') }}
	WHERE svIsNullApptPdctDurtC = 'Y'
)

SELECT * FROM Xfm__ToErrApptDurtId