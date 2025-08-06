{{ config(materialized='view', tags=['ExtAppProdComAppProdClp']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.APP_PROD_ID)) THEN (XfmSeparateRejects.APP_PROD_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.APP_PROD_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.APP_PROD_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		APP_PROD_ID,
		COM_SUBTYPE_CODE,
		CAMPAIGN_CAT_ID,
		COM_APP_ID,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec