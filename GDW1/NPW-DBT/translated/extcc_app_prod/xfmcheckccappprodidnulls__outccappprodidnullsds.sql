{{ config(materialized='view', tags=['ExtCC_APP_PROD']) }}

WITH XfmCheckCCAppProdIdNulls__OutCCAppProdIdNullsDS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmCheckCCAppProdIdNulls.CC_APP_PROD_ID)) THEN (XfmCheckCCAppProdIdNulls.CC_APP_PROD_ID) ELSE ""))) = 0) Then 'REJ2001' Else '',
		IFF(LEN(TRIM(IFF({{ ref('Remove_Duplicates_214') }}.CC_APP_PROD_ID IS NOT NULL, {{ ref('Remove_Duplicates_214') }}.CC_APP_PROD_ID, ''))) = 0, 'REJ2001', '') AS ErrorCode,
		CC_APP_PROD_ID,
		REQUESTED_LIMIT_AMT,
		CC_INTEREST_OPT_CAT_ID,
		CBA_HOMELOAN_NO,
		PRE_APPRV_AMOUNT,
		ETL_PROCESS_DT AS ETL_D,
		ETL_PROCESS_DT AS ORIG_ETL_D,
		ErrorCode AS EROR_C,
		READ_COSTS_AND_RISKS_FLAG,
		ACCEPTS_COSTS_AND_RISKS_DATE
	FROM {{ ref('Remove_Duplicates_214') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) = 'REJ'
)

SELECT * FROM XfmCheckCCAppProdIdNulls__OutCCAppProdIdNullsDS