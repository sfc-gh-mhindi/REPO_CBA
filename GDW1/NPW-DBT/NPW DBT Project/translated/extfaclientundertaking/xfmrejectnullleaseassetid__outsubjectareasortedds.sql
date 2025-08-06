{{ config(materialized='view', tags=['ExtFAClientUndertaking']) }}

WITH XfmRejectNullLeaseAssetId__OutSubjectAreaSortedDS AS (
	SELECT
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((ToXfmRejectNull.FA_CLIENT_UNDERTAKING_ID)) THEN (ToXfmRejectNull.FA_CLIENT_UNDERTAKING_ID) ELSE ""))) = 0 Then 'REJ6100' Else '',
		IFF(LEN(TRIM(IFF({{ ref('CpySubjectAreaNameSeq') }}.FA_CLIENT_UNDERTAKING_ID IS NOT NULL, {{ ref('CpySubjectAreaNameSeq') }}.FA_CLIENT_UNDERTAKING_ID, ''))) = 0, 'REJ6100', '') AS ErrorCode,
		FA_CLIENT_UNDERTAKING_ID,
		FA_UNDERTAKING_ID,
		COIN_ENTITY_ID,
		CLIENT_CORRELATION_ID,
		FA_ENTITY_CAT_ID,
		FA_CHILD_STATUS_CAT_ID,
		CLIENT_RELATIONSHIP_TYPE_ID,
		CLIENT_POSITION,
		IS_PRIMARY_FLAG,
		CIF_CODE
	FROM {{ ref('CpySubjectAreaNameSeq') }}
	WHERE ErrorCode <> 'REJ6100'
)

SELECT * FROM XfmRejectNullLeaseAssetId__OutSubjectAreaSortedDS