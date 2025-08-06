{{ config(materialized='view', tags=['ExtComBusAppProdClientRole']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.APP_PROD_CLIENT_ROLE_ID)) THEN (XfmSeparateRejects.APP_PROD_CLIENT_ROLE_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.APP_PROD_CLIENT_ROLE_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.APP_PROD_CLIENT_ROLE_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('JoinSrcSortReject') }}.APP_PROD_CLIENT_ROLE_ID_R AS APP_PROD_CLIENT_ROLE_ID,
		{{ ref('JoinSrcSortReject') }}.ROLE_CAT_ID_R AS ROLE_CAT_ID,
		{{ ref('JoinSrcSortReject') }}.CIF_CODE_R AS CIF_CODE,
		{{ ref('JoinSrcSortReject') }}.APP_PROD_ID_R AS APP_PROD_ID,
		{{ ref('JoinSrcSortReject') }}.SUBTYPE_CODE_R AS SUBTYPE_CODE,
		{{ ref('JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InRejectWithoutSourceRec