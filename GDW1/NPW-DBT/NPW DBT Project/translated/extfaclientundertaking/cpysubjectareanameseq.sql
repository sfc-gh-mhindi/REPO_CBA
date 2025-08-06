{{ config(materialized='view', tags=['ExtFAClientUndertaking']) }}

WITH CpySubjectAreaNameSeq AS (
	SELECT
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
	FROM {{ ref('SrcFAClientUndertaking') }}
)

SELECT * FROM CpySubjectAreaNameSeq