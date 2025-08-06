{{ config(materialized='view', tags=['XfmPlIntRateAmtMarginFrmExt']) }}

WITH XfmBusinessRules__OutErrorMAP_GRD_GNRC_MAP_FEAT_ISeq AS (
	SELECT
		-- *SRC*: \(20)If (InXfmBusinessRules.PL_INT_RATE_FOUND_FLAG = 'N' or (Len(Trim(( IF IsNotNull((InXfmBusinessRules.PL_INT_RATE_VARIANT_CAT_ID)) THEN (InXfmBusinessRules.PL_INT_RATE_VARIANT_CAT_ID) ELSE ""))) = 0) or (Len(Trim(( IF IsNotNull((InXfmBusinessRules.PL_INT_RATE_INT_RATE_FREQ_ID)) THEN (InXfmBusinessRules.PL_INT_RATE_INT_RATE_FREQ_ID) ELSE ""))) = 0)) then DEFAULT_NULL_VALUE else  if Len(Trim(( IF IsNotNull((InXfmBusinessRules.TARG_CHAR_C)) THEN (InXfmBusinessRules.TARG_CHAR_C) ELSE ""))) = 0 Then '999009' else InXfmBusinessRules.TARG_CHAR_C,
		IFF(    
	    {{ ref('ModNullHandling') }}.PL_INT_RATE_FOUND_FLAG = 'N'
	    or LEN(TRIM(IFF({{ ref('ModNullHandling') }}.PL_INT_RATE_VARIANT_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.PL_INT_RATE_VARIANT_CAT_ID, ''))) = 0
	    or LEN(TRIM(IFF({{ ref('ModNullHandling') }}.PL_INT_RATE_INT_RATE_FREQ_ID IS NOT NULL, {{ ref('ModNullHandling') }}.PL_INT_RATE_INT_RATE_FREQ_ID, ''))) = 0, 
	    DEFAULT_NULL_VALUE, 
	    IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.TARG_CHAR_C IS NOT NULL, {{ ref('ModNullHandling') }}.TARG_CHAR_C, ''))) = 0, '999009', {{ ref('ModNullHandling') }}.TARG_CHAR_C)
	) AS FeatI,
		-- *SRC*: \(20)If (InXfmBusinessRules.PL_MARGIN_FOUND_FLAG = 'N' or (Len(Trim(( IF IsNotNull((InXfmBusinessRules.PL_MARGIN_MARGIN_RESN_CAT_ID)) THEN (InXfmBusinessRules.PL_MARGIN_MARGIN_RESN_CAT_ID) ELSE ""))) = 0)) then DEFAULT_NULL_VALUE else InXfmBusinessRules.FEAT_OVRD_REAS_C,
		IFF({{ ref('ModNullHandling') }}.PL_MARGIN_FOUND_FLAG = 'N' OR LEN(TRIM(IFF({{ ref('ModNullHandling') }}.PL_MARGIN_MARGIN_RESN_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.PL_MARGIN_MARGIN_RESN_CAT_ID, ''))) = 0, DEFAULT_NULL_VALUE, {{ ref('ModNullHandling') }}.FEAT_OVRD_REAS_C) AS FeatOvrdReasC,
		-- *SRC*: \(20)If FeatI = '999009' then 'RPR5114' Else  If FeatOvrdReasC = '9999' then 'RPR5115' Else '',
		IFF(FeatI = '999009', 'RPR5114', IFF(FeatOvrdReasC = '9999', 'RPR5115', '')) AS ErrorCode,
		-- *SRC*: \(20)If ErrorCode <> "" Then @TRUE Else @FALSE,
		IFF(ErrorCode <> '', @TRUE, @FALSE) AS RejectFlag,
		{{ ref('ModNullHandling') }}.PL_INT_RATE_ID AS SRCE_KEY_I,
		'PL_INT_RATE_VARIANT_CAT_ID_FREQUENCY_ID' AS CONV_M,
		'LOOKUP' AS CONV_MAP_RULE_M,
		'MAP_GRD_GNRC_MAP_FEAT_I' AS TRSF_TABL_M,
		-- *SRC*: InXfmBusinessRules.PL_INT_RATE_VARIANT_CAT_ID : InXfmBusinessRules.PL_INT_RATE_INT_RATE_FREQ_ID,
		CONCAT({{ ref('ModNullHandling') }}.PL_INT_RATE_VARIANT_CAT_ID, {{ ref('ModNullHandling') }}.PL_INT_RATE_INT_RATE_FREQ_ID) AS VALU_CHNG_BFOR_X,
		FeatI AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'FEAT_I' AS TRSF_COLM_M
	FROM {{ ref('ModNullHandling') }}
	WHERE FeatI = '999009'
)

SELECT * FROM XfmBusinessRules__OutErrorMAP_GRD_GNRC_MAP_FEAT_ISeq