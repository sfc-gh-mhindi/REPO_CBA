{{ config(materialized='view', tags=['ExtPL_APP_PROD']) }}

WITH XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmCheckPLAppProdIdNulls.PL_APP_PROD_ID)) THEN (XfmCheckPLAppProdIdNulls.PL_APP_PROD_ID) ELSE ""))) = 0) Then 'REJ1005' Else '',
		IFF(LEN(TRIM(IFF({{ ref('Remove_Duplicates_220') }}.PL_APP_PROD_ID IS NOT NULL, {{ ref('Remove_Duplicates_220') }}.PL_APP_PROD_ID, ''))) = 0, 'REJ1005', '') AS ErrorCode,
		PL_APP_PROD_ID,
		PL_TARGET_CAT_ID,
		REPAY_APPROX_AMT,
		REPAY_FREQUENCY_ID,
		PL_APP_PROD_REPAY_CAT_ID,
		PL_PROD_TERM_CAT_ID,
		PL_CAMPAIGN_CAT_ID,
		AD_HOC_CAMPAIGN_DESC,
		CAR_SEEKER_FLAG,
		PL_PROD_TARGET_CAT_ID,
		PL_MARKETING_SOURCE_CAT_ID,
		HLS_ACCT_NO,
		TOTAL_INTEREST_AMT,
		APP_PROD_AMT,
		TP_BROKER_ID,
		TP_BROKER_FIRST_NAME,
		TP_BROKER_LAST_NAME,
		TP_BROKER_GROUP_CAT_ID,
		READ_COSTS_AND_RISKS_FLAG,
		ACCEPTS_COSTS_AND_RISKS_DATE
	FROM {{ ref('Remove_Duplicates_220') }}
	WHERE SUBSTRING(ErrorCode, 1, 3) <> 'REJ'
)

SELECT * FROM XfmCheckCCAppProdIdNulls__OutCheckPLAppProdIdNullsSorted