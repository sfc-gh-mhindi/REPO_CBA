{{ config(materialized='view', tags=['ExtCC_APP_PROD_BAL_XFER']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((XfmSeparateRejects.CC_APP_PROD_BAL_XFER_ID)) THEN (XfmSeparateRejects.CC_APP_PROD_BAL_XFER_ID) ELSE ""))) = 0 Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.CC_APP_PROD_BAL_XFER_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.CC_APP_PROD_BAL_XFER_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.CC_APP_PROD_BAL_XFER_ID_R AS CC_APP_PROD_BAL_XFER_ID,
		{{ ref('JoinSrcSortReject') }}.BAL_XFER_OPTION_CAT_ID_R AS BAL_XFER_OPTION_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.XFER_AMT_R AS XFER_AMT,
		{{ ref('JoinSrcSortReject') }}.BAL_XFER_INSTITUTION_CAT_ID_R AS BAL_XFER_INSTITUTION_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.CC_APP_PROD_ID_R AS CC_APP_PROD_ID,
		{{ ref('JoinSrcSortReject') }}.CC_APP_ID_R AS CC_APP_ID,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec