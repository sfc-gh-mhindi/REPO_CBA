{{ config(materialized='view', tags=['ExtFAClientUndertaking']) }}

WITH XfmIdentifyColsValue__ToBasedonR AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((OutSrcRejectMerge1.FA_CLIENT_UNDERTAKING_ID)) THEN (OutSrcRejectMerge1.FA_CLIENT_UNDERTAKING_ID) ELSE ""))) = 0) Then 'R' Else 'S',
		IFF(LEN(TRIM(IFF({{ ref('Copy_of_JoinSrcSortReject') }}.FA_CLIENT_UNDERTAKING_ID IS NOT NULL, {{ ref('Copy_of_JoinSrcSortReject') }}.FA_CLIENT_UNDERTAKING_ID, ''))) = 0, 'R', 'S') AS DeltaFlag,
		{{ ref('Copy_of_JoinSrcSortReject') }}.FA_CLIENT_UNDERTAKING_ID_R AS FA_CLIENT_UNDERTAKING_ID,
		{{ ref('Copy_of_JoinSrcSortReject') }}.FA_UNDERTAKING_ID_R AS FA_UNDERTAKING_ID,
		{{ ref('Copy_of_JoinSrcSortReject') }}.COIN_ENTITY_ID_R AS COIN_ENTITY_ID,
		{{ ref('Copy_of_JoinSrcSortReject') }}.CLIENT_CORRELATION_ID_R AS CLIENT_CORRELATION_ID,
		{{ ref('Copy_of_JoinSrcSortReject') }}.FA_ENTITY_CAT_ID_R AS FA_ENTITY_CAT_ID,
		{{ ref('Copy_of_JoinSrcSortReject') }}.FA_CHILD_STATUS_CAT_ID_R AS FA_CHILD_STATUS_CAT_ID,
		{{ ref('Copy_of_JoinSrcSortReject') }}.CLIENT_RELATIONSHIP_TYPE_ID_R AS CLIENT_RELATIONSHIP_TYPE_ID,
		{{ ref('Copy_of_JoinSrcSortReject') }}.CLIENT_POSITION_R AS CLIENT_POSITION,
		{{ ref('Copy_of_JoinSrcSortReject') }}.IS_PRIMARY_FLAG_R AS IS_PRIMARY_FLAG,
		{{ ref('Copy_of_JoinSrcSortReject') }}.CIF_CODE_R AS CIF_CODE,
		{{ ref('Copy_of_JoinSrcSortReject') }}.ORIG_ETL_D_R AS ORIG_ETL_D
	FROM {{ ref('Copy_of_JoinSrcSortReject') }}
	WHERE DeltaFlag = 'R'
)

SELECT * FROM XfmIdentifyColsValue__ToBasedonR