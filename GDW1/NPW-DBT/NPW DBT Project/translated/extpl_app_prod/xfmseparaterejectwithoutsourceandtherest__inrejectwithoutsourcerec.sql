{{ config(materialized='view', tags=['ExtPL_APP_PROD']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.PL_APP_PROD_ID)) THEN (XfmSeparateRejects.PL_APP_PROD_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.PL_APP_PROD_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.PL_APP_PROD_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.PL_APP_PROD_ID_R AS PL_APP_PROD_ID,
		{{ ref('JoinSrcSortReject') }}.PL_TARGET_CAT_ID_R AS PL_TARGET_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.REPAY_APPROX_AMT_R AS REPAY_APPROX_AMT,
		{{ ref('JoinSrcSortReject') }}.REPAY_FREQUENCY_ID_R AS REPAY_FREQUENCY_ID,
		{{ ref('JoinSrcSortReject') }}.PL_APP_PROD_REPAY_CAT_ID_R AS PL_APP_PROD_REPAY_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.PL_PROD_TERM_CAT_ID_R AS PL_PROD_TERM_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.PL_CAMPAIGN_CAT_ID_R AS PL_CAMPAIGN_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.AD_HOC_CAMPAIGN_DESC_R AS AD_HOC_CAMPAIGN_DESC,
		{{ ref('JoinSrcSortReject') }}.CAR_SEEKER_FLAG_R AS CAR_SEEKER_FLAG,
		{{ ref('JoinSrcSortReject') }}.PL_PROD_TARGET_CAT_ID_R AS PL_PROD_TARGET_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.PL_MARKETING_SOURCE_CAT_ID_R AS PL_MARKETING_SOURCE_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.HLS_ACCT_NO_R AS HLS_ACCT_NO,
		{{ ref('JoinSrcSortReject') }}.TOTAL_INTEREST_AMT_R AS TOTAL_INTEREST_AMT,
		{{ ref('JoinSrcSortReject') }}.APP_PROD_AMT_R AS APP_PROD_AMT,
		{{ ref('JoinSrcSortReject') }}.TP_BROKER_ID_R AS TP_BROKER_ID,
		{{ ref('JoinSrcSortReject') }}.TP_BROKER_FIRST_NAME_R AS TP_BROKER_FIRST_NAME,
		{{ ref('JoinSrcSortReject') }}.TP_BROKER_LAST_NAME_R AS TP_BROKER_LAST_NAME,
		{{ ref('JoinSrcSortReject') }}.TP_BROKER_GROUP_CAT_ID_R AS TP_BROKER_GROUP_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.READ_COSTS_AND_RISKS_FLAG_R AS READ_COSTS_AND_RISKS_FLAG,
		{{ ref('JoinSrcSortReject') }}.ACCEPTS_COSTS_AND_RISKS_DATE_R AS ACCEPTS_COSTS_AND_RISKS_DATE,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec