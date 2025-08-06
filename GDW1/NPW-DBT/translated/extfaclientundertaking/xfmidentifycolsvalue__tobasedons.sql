{{ config(materialized='view', tags=['ExtFAClientUndertaking']) }}

WITH XfmIdentifyColsValue__ToBasedonS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((OutSrcRejectMerge1.FA_CLIENT_UNDERTAKING_ID)) THEN (OutSrcRejectMerge1.FA_CLIENT_UNDERTAKING_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('Copy_of_JoinSrcSortReject') }}.FA_CLIENT_UNDERTAKING_ID IS NOT NULL, {{ ref('Copy_of_JoinSrcSortReject') }}.FA_CLIENT_UNDERTAKING_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		FA_CLIENT_UNDERTAKING_ID,
		FA_UNDERTAKING_ID,
		COIN_ENTITY_ID,
		CLIENT_CORRELATION_ID,
		FA_ENTITY_CAT_ID,
		FA_CHILD_STATUS_CAT_ID,
		CLIENT_RELATIONSHIP_TYPE_ID,
		CLIENT_POSITION,
		IS_PRIMARY_FLAG,
		CIF_CODE,
		ETL_PROCESS_DT AS ORIG_ETL_D
	FROM {{ ref('Copy_of_JoinSrcSortReject') }}
	WHERE DeltaFlag = 'S'
)

SELECT * FROM XfmIdentifyColsValue__ToBasedonS