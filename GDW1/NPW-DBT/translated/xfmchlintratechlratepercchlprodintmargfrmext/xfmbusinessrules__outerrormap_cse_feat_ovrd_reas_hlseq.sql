{{ config(materialized='view', tags=['XfmChlIntRateChlRatePercChlProdIntMargFrmExt']) }}

WITH XfmBusinessRules__OutErrorMAP_CSE_FEAT_OVRD_REAS_HLSeq AS (
	SELECT
		-- *SRC*: \(20)If (InXfmBusinessRules.RATE_FOUND_FLAG = 'Y' And Len(Trim(( IF IsNotNull((InXfmBusinessRules.RATE_HL_APP_PROD_ID)) THEN (InXfmBusinessRules.RATE_HL_APP_PROD_ID) ELSE ""))) = 0) then 'N' else 'Y',
		IFF({{ ref('ModNullHandling') }}.RATE_FOUND_FLAG = 'Y' AND LEN(TRIM(IFF({{ ref('ModNullHandling') }}.RATE_HL_APP_PROD_ID IS NOT NULL, {{ ref('ModNullHandling') }}.RATE_HL_APP_PROD_ID, ''))) = 0, 'N', 'Y') AS LoadApptPdctI,
		-- *SRC*: \(20)If (InXfmBusinessRules.RATE_FOUND_FLAG = 'Y' And Len(Trim(( IF IsNotNull((InXfmBusinessRules.RATE_CASS_INT_RATE_TYPE_CODE)) THEN (InXfmBusinessRules.RATE_CASS_INT_RATE_TYPE_CODE) ELSE ""))) = 0) then 'N' Else 'Y',
		IFF({{ ref('ModNullHandling') }}.RATE_FOUND_FLAG = 'Y' AND LEN(TRIM(IFF({{ ref('ModNullHandling') }}.RATE_CASS_INT_RATE_TYPE_CODE IS NOT NULL, {{ ref('ModNullHandling') }}.RATE_CASS_INT_RATE_TYPE_CODE, ''))) = 0, 'N', 'Y') AS LoadFeatI,
		-- *SRC*: \(20)If InXfmBusinessRules.RATE_FOUND_FLAG = 'N' Or InXfmBusinessRules.PERC_FOUND_FLAG = 'N' Then DEFAULT_NULL_VALUE Else InXfmBusinessRules.FEAT_I,
		IFF({{ ref('ModNullHandling') }}.RATE_FOUND_FLAG = 'N' OR {{ ref('ModNullHandling') }}.PERC_FOUND_FLAG = 'N', DEFAULT_NULL_VALUE, {{ ref('ModNullHandling') }}.FEAT_I) AS FeatI,
		-- *SRC*: \(20)If (InXfmBusinessRules.MARG_FOUND_FLAG = 'N' Or (Len(Trim(( IF IsNotNull((InXfmBusinessRules.MARG_HL_PROD_INT_MARGIN_CAT_ID)) THEN (InXfmBusinessRules.MARG_HL_PROD_INT_MARGIN_CAT_ID) ELSE ""))) = 0)) then DEFAULT_NULL_VALUE Else InXfmBusinessRules.FEAT_OVRD_REAS_C,
		IFF({{ ref('ModNullHandling') }}.MARG_FOUND_FLAG = 'N' OR LEN(TRIM(IFF({{ ref('ModNullHandling') }}.MARG_HL_PROD_INT_MARGIN_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.MARG_HL_PROD_INT_MARGIN_CAT_ID, ''))) = 0, DEFAULT_NULL_VALUE, {{ ref('ModNullHandling') }}.FEAT_OVRD_REAS_C) AS FeatOvrdReasC,
		-- *SRC*: \(20)If LoadApptPdctI = 'N' Then 'REJ4104' Else  If LoadFeatI = 'N' Then 'REJ4105' Else  If FeatI = '999009' then 'RPR4006' Else  If FeatOvrdReasC = '9999' then 'RPR4007' Else '',
		IFF(LoadApptPdctI = 'N', 'REJ4104', IFF(LoadFeatI = 'N', 'REJ4105', IFF(FeatI = '999009', 'RPR4006', IFF(FeatOvrdReasC = '9999', 'RPR4007', '')))) AS ErrorCode,
		-- *SRC*: \(20)If ErrorCode <> "" Then @TRUE Else @FALSE,
		IFF(ErrorCode <> '', @TRUE, @FALSE) AS RejectFlag,
		{{ ref('ModNullHandling') }}.HL_INT_RATE_ID AS SRCE_KEY_I,
		'HL_PROD_INT_MARGIN_CAT_ID' AS CONV_M,
		'LOOKUP' AS CONV_MAP_RULE_M,
		'MAP_CSE_FEAT_OVRD_REAS_HL' AS TRSF_TABL_M,
		-- *SRC*: ( IF IsNotNull((InXfmBusinessRules.MARG_HL_PROD_INT_MARGIN_CAT_ID)) THEN (InXfmBusinessRules.MARG_HL_PROD_INT_MARGIN_CAT_ID) ELSE ""),
		IFF({{ ref('ModNullHandling') }}.MARG_HL_PROD_INT_MARGIN_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.MARG_HL_PROD_INT_MARGIN_CAT_ID, '') AS VALU_CHNG_BFOR_X,
		{{ ref('ModNullHandling') }}.FEAT_OVRD_REAS_C AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'FEAT_OVRD_REAS_C' AS TRSF_COLM_M
	FROM {{ ref('ModNullHandling') }}
	WHERE FeatOvrdReasC = '9999'
)

SELECT * FROM XfmBusinessRules__OutErrorMAP_CSE_FEAT_OVRD_REAS_HLSeq