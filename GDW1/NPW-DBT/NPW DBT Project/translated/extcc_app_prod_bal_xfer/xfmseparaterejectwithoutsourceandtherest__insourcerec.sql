{{ config(materialized='view', tags=['ExtCC_APP_PROD_BAL_XFER']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((XfmSeparateRejects.CC_APP_PROD_BAL_XFER_ID)) THEN (XfmSeparateRejects.CC_APP_PROD_BAL_XFER_ID) ELSE ""))) = 0 Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.CC_APP_PROD_BAL_XFER_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.CC_APP_PROD_BAL_XFER_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		CC_APP_PROD_BAL_XFER_ID,
		BAL_XFER_OPTION_CAT_ID,
		XFER_AMT,
		BAL_XFER_INSTITUTION_CAT_ID,
		CC_APP_PROD_ID,
		CC_APP_ID,
		ETL_PROCESS_DT AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec