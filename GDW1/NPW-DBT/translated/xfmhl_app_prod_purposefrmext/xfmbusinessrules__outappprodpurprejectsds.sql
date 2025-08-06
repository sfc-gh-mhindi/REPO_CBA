{{ config(materialized='view', tags=['XfmHL_APP_PROD_PURPOSEFrmExt']) }}

WITH XfmBusinessRules__OutAppProdPurpRejectsDS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.HL_LOAN_PURPOSE_CAT_ID)) THEN (InXfmBusinessRules.HL_LOAN_PURPOSE_CAT_ID) ELSE ""))) = 0) Then DEFAULT_NULL_VALUE ELSE InXfmBusinessRules.PURP_TYPE_C,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.HL_LOAN_PURPOSE_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.HL_LOAN_PURPOSE_CAT_ID, ''))) = 0, DEFAULT_NULL_VALUE, {{ ref('ModNullHandling') }}.PURP_TYPE_C) AS svApptPurp,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.HL_LOAN_PURPOSE_CAT_ID)) THEN (InXfmBusinessRules.HL_LOAN_PURPOSE_CAT_ID) ELSE ""))) = 0) Then 'N' Else 'Y',
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.HL_LOAN_PURPOSE_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.HL_LOAN_PURPOSE_CAT_ID, ''))) = 0, 'N', 'Y') AS svLoadApptPdctPurp,
		-- *SRC*: \(20)If svApptPurp = '9999' then 'RPR2107' else  if svApptPurp = DEFAULT_NULL_VALUE then 'REJ2013' else '',
		IFF(svApptPurp = '9999', 'RPR2107', IFF(svApptPurp = DEFAULT_NULL_VALUE, 'REJ2013', '')) AS svErrorCode,
		-- *SRC*: \(20)If svErrorCode <> "" Then @TRUE Else @FALSE,
		IFF(svErrorCode <> '', @TRUE, @FALSE) AS svRejectFlag,
		HL_APP_PROD_PURPOSE_ID,
		HL_APP_PROD_ID,
		HL_LOAN_PURPOSE_CAT_ID,
		AMOUNT,
		MAIN_PURPOSE,
		ETL_PROCESS_DT AS ETL_D,
		ORIG_ETL_D,
		svErrorCode AS EROR_C
	FROM {{ ref('ModNullHandling') }}
	WHERE svRejectFlag
)

SELECT * FROM XfmBusinessRules__OutAppProdPurpRejectsDS