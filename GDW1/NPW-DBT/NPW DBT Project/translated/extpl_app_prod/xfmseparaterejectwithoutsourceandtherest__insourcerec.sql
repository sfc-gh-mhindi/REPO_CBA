{{ config(materialized='view', tags=['ExtPL_APP_PROD']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.PL_APP_PROD_ID)) THEN (XfmSeparateRejects.PL_APP_PROD_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.PL_APP_PROD_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.PL_APP_PROD_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
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
		ACCEPTS_COSTS_AND_RISKS_DATE,
		ETL_PROCESS_DT AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec