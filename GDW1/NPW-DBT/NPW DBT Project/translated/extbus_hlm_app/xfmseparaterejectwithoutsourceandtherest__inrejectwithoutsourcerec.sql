{{ config(materialized='view', tags=['ExtBUS_HLM_APP']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If IsNUll(XfmSeparateRejects.APP_ID) Then 'R' Else 'S',
		IFF({{ ref('JoinSrcSortReject') }}.APP_ID IS NULL, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.rightRec_APP_ID AS APP_ID,
		{{ ref('JoinSrcSortReject') }}.HLM_ACCOUNT_ID_R AS HLM_ACCOUNT_ID,
		{{ ref('JoinSrcSortReject') }}.ACCOUNT_NUMBER_R AS ACCOUNT_NUMBER,
		{{ ref('JoinSrcSortReject') }}.CRIS_PRODUCT_ID_R AS CRIS_PRODUCT_ID,
		{{ ref('JoinSrcSortReject') }}.HLM_APP_TYPE_CAT_ID_R AS HLM_APP_TYPE_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.DISCHARGE_REASON_ID_R AS DCHG_REAS_ID,
		{{ ref('JoinSrcSortReject') }}.HL_APP_PROD_ID_R AS HL_APP_PROD_ID,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec