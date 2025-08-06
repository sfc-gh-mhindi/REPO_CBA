{{ config(materialized='view', tags=['ExtCC_APP_PROD']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.CC_APP_PROD_ID)) THEN (XfmSeparateRejects.CC_APP_PROD_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.CC_APP_PROD_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.CC_APP_PROD_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.CC_APP_PROD_ID_R AS CC_APP_PROD_ID,
		{{ ref('JoinSrcSortReject') }}.REQUESTED_LIMIT_AMT_R AS REQUESTED_LIMIT_AMT,
		{{ ref('JoinSrcSortReject') }}.CC_INTEREST_OPT_CAT_ID_R AS CC_INTEREST_OPT_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.CBA_HOMELOAN_NO_R AS CBA_HOMELOAN_NO,
		{{ ref('JoinSrcSortReject') }}.PRE_APPRV_AMOUNT_R AS PRE_APPRV_AMOUNT,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D,
		{{ ref('JoinSrcSortReject') }}.READ_COSTS_AND_RISKS_FLAG_R AS READ_COSTS_AND_RISKS_FLAG,
		{{ ref('JoinSrcSortReject') }}.ACCEPTS_COSTS_AND_RISKS_DATE_R AS ACCEPTS_COSTS_AND_RISKS_DATE,
		-- *SRC*: SetNull(),
		SETNULL() AS NTM_CAMPAIGN_ID,
		-- *SRC*: SetNull(),
		SETNULL() AS FRIES_CAMPAIGN_CODE,
		-- *SRC*: SetNull(),
		SETNULL() AS OAP_CAMPAIGN_CODE
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec