{{ config(materialized='view', tags=['XfmPlFeePlMarginFrmExt']) }}

WITH XfmBusinessRules__OutPlFeePlMarginRejectsDS AS (
	SELECT
		-- *SRC*: \(20)If InXfmBusinessRules.PL_FEE_FOUND_FLAG = 'N' Then DEFAULT_NULL_VALUE Else InXfmBusinessRules.FEAT_I,
		IFF({{ ref('ModNullHandling') }}.PL_FEE_FOUND_FLAG = 'N', DEFAULT_NULL_VALUE, {{ ref('ModNullHandling') }}.FEAT_I) AS FeatI,
		-- *SRC*: \(20)If (InXfmBusinessRules.PL_FEE_FOUND_FLAG = 'Y' And (Len(Trim(( IF IsNotNull((InXfmBusinessRules.PL_FEE_FEE_SCREEN_DESC)) THEN (InXfmBusinessRules.PL_FEE_FEE_SCREEN_DESC) ELSE ""))) = 0)) then 'N' Else 'Y',
		IFF({{ ref('ModNullHandling') }}.PL_FEE_FOUND_FLAG = 'Y' AND LEN(TRIM(IFF({{ ref('ModNullHandling') }}.PL_FEE_FEE_SCREEN_DESC IS NOT NULL, {{ ref('ModNullHandling') }}.PL_FEE_FEE_SCREEN_DESC, ''))) = 0, 'N', 'Y') AS LoadFeatI,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.PL_APP_PROD_ID)) THEN (InXfmBusinessRules.PL_APP_PROD_ID) ELSE ""))) = 0) then 'N' else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.PL_APP_PROD_ID IS NOT NULL, {{ ref('ModNullHandling') }}.PL_APP_PROD_ID, ''))) = 0, 'N', 'Y') AS LoadApptPdctI,
		-- *SRC*: \(20)If (InXfmBusinessRules.PL_MARGIN_FOUND_FLAG = 'N' or (Len(Trim(( IF IsNotNull((InXfmBusinessRules.PL_MARGIN_MARGIN_REASON_CAT_ID)) THEN (InXfmBusinessRules.PL_MARGIN_MARGIN_REASON_CAT_ID) ELSE ""))) = 0)) then DEFAULT_NULL_VALUE else InXfmBusinessRules.FEAT_OVRD_REAS_C,
		IFF({{ ref('ModNullHandling') }}.PL_MARGIN_FOUND_FLAG = 'N' OR LEN(TRIM(IFF({{ ref('ModNullHandling') }}.PL_MARGIN_MARGIN_REASON_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.PL_MARGIN_MARGIN_REASON_CAT_ID, ''))) = 0, DEFAULT_NULL_VALUE, {{ ref('ModNullHandling') }}.FEAT_OVRD_REAS_C) AS FeatOvrdReasC,
		-- *SRC*: \(20)If (InXfmBusinessRules.PL_FEE_FOUND_FLAG = 'N' or (Len(Trim(( IF IsNotNull((InXfmBusinessRules.PL_FEE_PL_CAPITALIS_FEE_CAT_ID)) THEN (InXfmBusinessRules.PL_FEE_PL_CAPITALIS_FEE_CAT_ID) ELSE ""))) = 0)) then ' ' else InXfmBusinessRules.FEE_CAPL_F,
		IFF({{ ref('ModNullHandling') }}.PL_FEE_FOUND_FLAG = 'N' OR LEN(TRIM(IFF({{ ref('ModNullHandling') }}.PL_FEE_PL_CAPITALIS_FEE_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.PL_FEE_PL_CAPITALIS_FEE_CAT_ID, ''))) = 0, ' ', {{ ref('ModNullHandling') }}.FEE_CAPL_F) AS FeeCaplF,
		-- *SRC*: \(20)If (InXfmBusinessRules.PL_FEE_FOUND_FLAG = 'N') then 'Y' Else ( If Len(Trim(( IF IsNotNull((InXfmBusinessRules.PL_FEE_START_DATE)) THEN (InXfmBusinessRules.PL_FEE_START_DATE) ELSE ""))) = 0 then 'Y' else 'N'),
		IFF({{ ref('ModNullHandling') }}.PL_FEE_FOUND_FLAG = 'N', 'Y', IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.PL_FEE_START_DATE IS NOT NULL, {{ ref('ModNullHandling') }}.PL_FEE_START_DATE, ''))) = 0, 'Y', 'N')) AS StartDtIsNull,
		-- *SRC*: \(20)If (StartDtIsNull = 'N') then ( if IsValid('date', StringToDate(InXfmBusinessRules.PL_FEE_START_DATE, '%yyyy%mm%dd')) Then 'N' Else 'Y') Else 'N',
		IFF(StartDtIsNull = 'N', IFF(ISVALID('date', STRINGTODATE({{ ref('ModNullHandling') }}.PL_FEE_START_DATE, '%yyyy%mm%dd')), 'N', 'Y'), 'N') AS ErrorStartDt,
		-- *SRC*: \(20)If LoadFeatI = 'N' Then 'REJ1003' Else  If LoadApptPdctI = 'N' Then 'REJ1004' Else  If FeatI = '999010' Then 'RPR1100' Else  If FeatOvrdReasC = '9999' Then 'RPR1101' Else  If FeeCaplF = '9' Then 'RPR1102' Else '',
		IFF(LoadFeatI = 'N', 'REJ1003', IFF(LoadApptPdctI = 'N', 'REJ1004', IFF(FeatI = '999010', 'RPR1100', IFF(FeatOvrdReasC = '9999', 'RPR1101', IFF(FeeCaplF = '9', 'RPR1102', ''))))) AS ErrorCode,
		-- *SRC*: \(20)If ErrorCode <> "" Then @TRUE Else @FALSE,
		IFF(ErrorCode <> '', @TRUE, @FALSE) AS RejectFlag,
		PL_FEE_ID,
		PL_APP_PROD_ID,
		PL_FEE_PL_FEE_ID,
		PL_FEE_ADD_TO_TOTAL_FLAG,
		PL_FEE_FEE_AMT,
		PL_FEE_BASE_FEE_AMT,
		PL_FEE_PAY_FEE_AT_FUNDING_FLAG,
		PL_FEE_START_DATE,
		PL_FEE_PL_CAPITALIS_FEE_CAT_ID,
		PL_FEE_FEE_SCREEN_DESC,
		PL_FEE_FEE_DESC,
		PL_FEE_CASS_FEE_CODE,
		PL_FEE_CASS_FEE_KEY,
		PL_FEE_TOTAL_FEE_AMT,
		PL_FEE_PL_APP_PROD_ID,
		PL_MARGIN_PL_MARGIN_ID,
		PL_MARGIN_MARGIN_AMT,
		PL_MARGIN_PL_FEE_ID,
		PL_MARGIN_PL_INT_RATE_ID,
		PL_MARGIN_MARGIN_REASON_CAT_ID,
		PL_MARGIN_PL_APP_PROD_ID,
		PL_FEE_FOUND_FLAG,
		PL_MARGIN_FOUND_FLAG,
		ETL_PROCESS_DT AS ETL_D,
		ORIG_ETL_D,
		ErrorCode AS EROR_C
	FROM {{ ref('ModNullHandling') }}
	WHERE RejectFlag
)

SELECT * FROM XfmBusinessRules__OutPlFeePlMarginRejectsDS