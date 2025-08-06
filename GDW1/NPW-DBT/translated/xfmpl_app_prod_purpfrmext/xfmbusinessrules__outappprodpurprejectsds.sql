{{ config(materialized='view', tags=['XfmPL_APP_PROD_PURPFrmExt']) }}

WITH XfmBusinessRules__OutAppProdPurpRejectsDS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((InXfmBusinessRules.PL_APP_PROD_PURP_CAT_ID)) THEN (InXfmBusinessRules.PL_APP_PROD_PURP_CAT_ID) ELSE ""))) = 0) Then 'UNKN' ELSE InXfmBusinessRules.PURP_TYPE_C,
		IFF(LEN(TRIM(IFF({{ ref('ModNullHandling') }}.PL_APP_PROD_PURP_CAT_ID IS NOT NULL, {{ ref('ModNullHandling') }}.PL_APP_PROD_PURP_CAT_ID, ''))) = 0, 'UNKN', {{ ref('ModNullHandling') }}.PURP_TYPE_C) AS svApptPurp,
		-- *SRC*: \(20)If svApptPurp = '9999' then 'RPR2105' else  if svApptPurp = DEFAULT_NULL_VALUE then 'REJ2008' else '',
		IFF(svApptPurp = '9999', 'RPR2105', IFF(svApptPurp = DEFAULT_NULL_VALUE, 'REJ2008', '')) AS svErrorCode,
		-- *SRC*: \(20)If svErrorCode <> "" Then @TRUE Else @FALSE,
		IFF(svErrorCode <> '', @TRUE, @FALSE) AS svRejectFlag,
		PL_APP_PROD_PURP_ID,
		PL_APP_PROD_PURP_CAT_ID,
		AMT,
		PL_APP_PROD_ID,
		ETL_PROCESS_DT AS ETL_D,
		ORIG_ETL_D,
		svErrorCode AS EROR_C
	FROM {{ ref('ModNullHandling') }}
	WHERE svRejectFlag
)

SELECT * FROM XfmBusinessRules__OutAppProdPurpRejectsDS