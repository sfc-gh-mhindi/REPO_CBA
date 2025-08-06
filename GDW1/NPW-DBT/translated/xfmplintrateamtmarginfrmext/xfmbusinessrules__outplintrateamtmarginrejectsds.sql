{{ config(materialized='view', tags=['XfmPlIntRateAmtMarginFrmExt']) }}

WITH XfmBusinessRules__OutPlIntRateAmtMarginRejectsDS AS (
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
		PL_INT_RATE_ID,
		PL_INT_RATE_PL_INT_RATE_ID,
		PL_INT_RATE_DOC_SEQ_NO,
		PL_INT_RATE_CASS_MARGIN_AMT,
		PL_INT_RATE_INT_RATE_TERM,
		PL_INT_RATE_INT_RATE_FREQ_ID,
		PL_INT_RATE_VARIANT_CAT_ID,
		PL_INT_RATE_USAGE_CAT_ID,
		PL_INT_RATE_PL_APP_PROD_ID,
		PL_MARGIN_PL_MARGIN_ID,
		PL_MARGIN_MARGIN_AMT,
		PL_MARGIN_PL_FEE_ID,
		PL_MARGIN_PL_INT_RATE_ID,
		PL_MARGIN_MARGIN_RESN_CAT_ID,
		PL_MARGIN_PL_APP_PROD_ID,
		PL_INT_RATE_AMT_INT_RTE_AMT_2,
		PL_INT_RATE_AMT_INT_RTCT_ID2,
		PL_INT_RATE_AMT_PL_INT_RATE_ID,
		PL_INT_RATE_AMT_INT_RATE_AMT_1,
		PL_INT_RATE_AMT_INT_RTCT_ID1,
		PL_MARGIN_FOUND_FLAG,
		PL_INT_RATE_FOUND_FLAG,
		PL_INT_RATE_AMT_FOUND_FLAG,
		ETL_PROCESS_DT AS ETL_D,
		ORIG_ETL_D,
		ErrorCode AS EROR_C
	FROM {{ ref('ModNullHandling') }}
	WHERE RejectFlag
)

SELECT * FROM XfmBusinessRules__OutPlIntRateAmtMarginRejectsDS