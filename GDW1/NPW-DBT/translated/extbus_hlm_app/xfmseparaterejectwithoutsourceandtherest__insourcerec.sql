{{ config(materialized='view', tags=['ExtBUS_HLM_APP']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If IsNUll(XfmSeparateRejects.APP_ID) Then 'R' Else 'S',
		IFF({{ ref('JoinSrcSortReject') }}.APP_ID IS NULL, 'R', 'S') AS DeltaFlag,
		APP_ID,
		HLM_ACCOUNT_ID,
		ACCOUNT_NUMBER,
		CRIS_PRODUCT_ID,
		HLM_APP_TYPE_CAT_ID,
		DCHG_REAS_ID,
		HL_APP_PROD_ID,
		ETL_PROCESS_DT AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec