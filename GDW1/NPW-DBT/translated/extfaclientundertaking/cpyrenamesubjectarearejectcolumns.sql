{{ config(materialized='view', tags=['ExtFAClientUndertaking']) }}

WITH CpyRenameSubjectAreaRejectColumns AS (
	SELECT
		FA_CLIENT_UNDERTAKING_ID,
		{{ ref('SrcSubjectAreaRejectOra') }}.FA_UNDERTAKING_ID AS FA_UNDERTAKING_ID_R,
		{{ ref('SrcSubjectAreaRejectOra') }}.COIN_ENTITY_ID AS COIN_ENTITY_ID_R,
		{{ ref('SrcSubjectAreaRejectOra') }}.CLIENT_CORRELATION_ID AS CLIENT_CORRELATION_ID_R,
		{{ ref('SrcSubjectAreaRejectOra') }}.FA_ENTITY_CAT_ID AS FA_ENTITY_CAT_ID_R,
		{{ ref('SrcSubjectAreaRejectOra') }}.FA_CHILD_STATUS_CAT_ID AS FA_CHILD_STATUS_CAT_ID_R,
		{{ ref('SrcSubjectAreaRejectOra') }}.CLIENT_RELATIONSHIP_TYPE_ID AS CLIENT_RELATIONSHIP_TYPE_ID_R,
		{{ ref('SrcSubjectAreaRejectOra') }}.CLIENT_POSITION AS CLIENT_POSITION_R,
		{{ ref('SrcSubjectAreaRejectOra') }}.IS_PRIMARY_FLAG AS IS_PRIMARY_FLAG_R,
		{{ ref('SrcSubjectAreaRejectOra') }}.CIF_CODE AS CIF_CODE_R,
		{{ ref('SrcSubjectAreaRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcSubjectAreaRejectOra') }}
)

SELECT * FROM CpyRenameSubjectAreaRejectColumns