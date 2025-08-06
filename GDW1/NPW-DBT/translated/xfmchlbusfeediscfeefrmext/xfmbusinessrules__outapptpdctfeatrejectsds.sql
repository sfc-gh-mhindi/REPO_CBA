{{ config(materialized='view', tags=['XfmChlBusFeeDiscFeeFrmExt']) }}

WITH XfmBusinessRules__OutApptPdctFeatRejectsDS AS (
	SELECT
		-- *SRC*: Len(Trim(( IF IsNotNull((InXfmBusinessRules.BFD_HL_FEE_DISCOUNT_ID)) THEN (InXfmBusinessRules.BFD_HL_FEE_DISCOUNT_ID) ELSE ""))) > 0,
		LEN(TRIM(IFF({{ ref('ModNullHandling') }}.BFD_HL_FEE_DISCOUNT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.BFD_HL_FEE_DISCOUNT_ID, ''))) > 0 AS svDiscountFeeFlag,
		-- *SRC*: InXfmBusinessRules.BF_FOUND_FLAG = 'Y' AND Len(Trim(( IF IsNotNull((InXfmBusinessRules.SRCE_CHAR_1_C)) THEN (InXfmBusinessRules.SRCE_CHAR_1_C) ELSE ""))) = 0,
		{{ ref('ModNullHandling') }}.BF_FOUND_FLAG = 'Y' AND LEN(TRIM(IFF({{ ref('ModNullHandling') }}.SRCE_CHAR_1_C IS NOT NULL, {{ ref('ModNullHandling') }}.SRCE_CHAR_1_C, ''))) = 0 AS svIgnoreFlag,
		-- *SRC*: \(20)if InXfmBusinessRules.BFD_FOUND_FLAG = 'Y' AND Not(svIgnoreFlag) AND (Len(Trim(( IF IsNotNull((InXfmBusinessRules.BFD_HL_FEE_DISCOUNT_CAT_ID)) THEN (InXfmBusinessRules.BFD_HL_FEE_DISCOUNT_CAT_ID) ELSE ""))) <> 0 AND InXfmBusinessRules.OVRD_REAS_C = '9999') Then 'Y' Else 'N',
		IFF({{ ref('ModNullHandling') }}.BFD_FOUND_FLAG = 'Y' AND Not svIgnoreFlag AND LEN(TRIM(IFF({{ ref('ModNullHandling') }}.BFD_HL_FEE_DISCOUNT_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.BFD_HL_FEE_DISCOUNT_CAT_ID, ''))) <> 0 AND {{ ref('ModNullHandling') }}.OVRD_REAS_C = '9999', 'Y', 'N') AS svErrorOvrdReasC,
		-- *SRC*: \(20)If InXfmBusinessRules.TARG_CHAR_C = "999010" AND InXfmBusinessRules.BF_FOUND_FLAG = "Y" Then "RPR4004" Else  If svErrorOvrdReasC = "Y" Then "RPR4009" Else '',
		IFF({{ ref('ModNullHandling') }}.TARG_CHAR_C = '999010' AND {{ ref('ModNullHandling') }}.BF_FOUND_FLAG = 'Y', 'RPR4004', IFF(svErrorOvrdReasC = 'Y', 'RPR4009', '')) AS svErrorCode,
		-- *SRC*: svErrorCode <> "",
		svErrorCode <> '' AS svRejectFlag,
		HL_FEE_ID,
		BF_HL_FEE_ID,
		BF_HL_APP_PROD_ID,
		{{ ref('ModNullHandling') }}.SRCE_CHAR_1_C AS BF_XML_CODE,
		BF_DISPLAY_NAME,
		BF_CATEGORY,
		BF_UNIT_AMOUNT,
		BF_TOTAL_AMOUNT,
		BFD_HL_FEE_DISCOUNT_ID,
		BFD_HL_FEE_ID,
		BFD_DISCOUNT_REASON,
		BFD_DISCOUNT_CODE,
		BFD_DISCOUNT_AMT,
		BFD_DISCOUNT_TERM,
		BFD_HL_FEE_DISCOUNT_CAT_ID,
		BF_FOUND_FLAG,
		BFD_FOUND_FLAG,
		ETL_PROCESS_DT AS ETL_D,
		ORIG_ETL_D,
		svErrorCode AS EROR_C
	FROM {{ ref('ModNullHandling') }}
	WHERE svRejectFlag
)

SELECT * FROM XfmBusinessRules__OutApptPdctFeatRejectsDS