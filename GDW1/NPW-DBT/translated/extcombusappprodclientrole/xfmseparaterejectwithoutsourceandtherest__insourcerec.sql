{{ config(materialized='view', tags=['ExtComBusAppProdClientRole']) }}

WITH XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((XfmSeparateRejects.APP_PROD_CLIENT_ROLE_ID)) THEN (XfmSeparateRejects.APP_PROD_CLIENT_ROLE_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('JoinSrcSortReject') }}.APP_PROD_CLIENT_ROLE_ID IS NOT NULL, {{ ref('JoinSrcSortReject') }}.APP_PROD_CLIENT_ROLE_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		APP_PROD_CLIENT_ROLE_ID,
		ROLE_CAT_ID,
		CIF_CODE,
		APP_PROD_ID,
		SUBTYPE_CODE,
		ETL_PROCESS_DT AS ORIG_ETL_D
	FROM {{ ref('JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmSeparateRejectWithoutSourceAndTheRest__InSourceRec