{{ config(materialized='view', tags=['ExtCC_APP_PROD']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.CC_APP_PROD_ID)) THEN (XfmSeparateRejects.CC_APP_PROD_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.CC_APP_PROD_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.CC_APP_PROD_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		CC_APP_PROD_ID,
		REQUESTED_LIMIT_AMT,
		CC_INTEREST_OPT_CAT_ID,
		CBA_HOMELOAN_NO,
		PRE_APPRV_AMOUNT,
		ETL_PROCESS_DT AS ORIG_ETL_D,
		READ_COSTS_AND_RISKS_FLAG,
		ACCEPTS_COSTS_AND_RISKS_DATE,
		NTM_CAMPAIGN_ID,
		FRIES_CAMPAIGN_CODE,
		OAP_CAMPAIGN_CODE
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec