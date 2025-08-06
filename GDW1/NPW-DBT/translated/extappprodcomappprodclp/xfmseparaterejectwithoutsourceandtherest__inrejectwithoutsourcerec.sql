{{ config(materialized='view', tags=['ExtAppProdComAppProdClp']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.APP_PROD_ID)) THEN (XfmSeparateRejects.APP_PROD_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.APP_PROD_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.APP_PROD_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.APP_PROD_ID_R AS APP_PROD_ID,
		{{ ref('JoinSrcSortReject') }}.COM_SUBTYPE_CODE_R AS COM_SUBTYPE_CODE,
		{{ ref('JoinSrcSortReject') }}.CAMPAIGN_CAT_ID_R AS CAMPAIGN_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.COM_APP_ID_R AS COM_APP_ID,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec