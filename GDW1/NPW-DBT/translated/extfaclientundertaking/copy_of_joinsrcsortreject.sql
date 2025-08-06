{{ config(materialized='view', tags=['ExtFAClientUndertaking']) }}

WITH Copy_of_JoinSrcSortReject AS (
	SELECT
		{{ ref('XfmRejectNullLeaseAssetId__OutSubjectAreaSortedDS') }}.FA_CLIENT_UNDERTAKING_ID,
		{{ ref('XfmRejectNullLeaseAssetId__OutSubjectAreaSortedDS') }}.FA_UNDERTAKING_ID,
		{{ ref('XfmRejectNullLeaseAssetId__OutSubjectAreaSortedDS') }}.COIN_ENTITY_ID,
		{{ ref('XfmRejectNullLeaseAssetId__OutSubjectAreaSortedDS') }}.CLIENT_CORRELATION_ID,
		{{ ref('XfmRejectNullLeaseAssetId__OutSubjectAreaSortedDS') }}.FA_ENTITY_CAT_ID,
		{{ ref('XfmRejectNullLeaseAssetId__OutSubjectAreaSortedDS') }}.FA_CHILD_STATUS_CAT_ID,
		{{ ref('XfmRejectNullLeaseAssetId__OutSubjectAreaSortedDS') }}.CLIENT_RELATIONSHIP_TYPE_ID,
		{{ ref('XfmRejectNullLeaseAssetId__OutSubjectAreaSortedDS') }}.CLIENT_POSITION,
		{{ ref('XfmRejectNullLeaseAssetId__OutSubjectAreaSortedDS') }}.IS_PRIMARY_FLAG,
		{{ ref('XfmRejectNullLeaseAssetId__OutSubjectAreaSortedDS') }}.CIF_CODE,
		{{ ref('CpyRenameSubjectAreaRejectColumns') }}.FA_CLIENT_UNDERTAKING_ID AS FA_CLIENT_UNDERTAKING_ID_R,
		{{ ref('CpyRenameSubjectAreaRejectColumns') }}.FA_UNDERTAKING_ID_R,
		{{ ref('CpyRenameSubjectAreaRejectColumns') }}.COIN_ENTITY_ID_R,
		{{ ref('CpyRenameSubjectAreaRejectColumns') }}.CLIENT_CORRELATION_ID_R,
		{{ ref('CpyRenameSubjectAreaRejectColumns') }}.FA_ENTITY_CAT_ID_R,
		{{ ref('CpyRenameSubjectAreaRejectColumns') }}.FA_CHILD_STATUS_CAT_ID_R,
		{{ ref('CpyRenameSubjectAreaRejectColumns') }}.CLIENT_RELATIONSHIP_TYPE_ID_R,
		{{ ref('CpyRenameSubjectAreaRejectColumns') }}.CLIENT_POSITION_R,
		{{ ref('CpyRenameSubjectAreaRejectColumns') }}.IS_PRIMARY_FLAG_R,
		{{ ref('CpyRenameSubjectAreaRejectColumns') }}.CIF_CODE_R,
		{{ ref('CpyRenameSubjectAreaRejectColumns') }}.ORIG_ETL_D_R
	FROM {{ ref('XfmRejectNullLeaseAssetId__OutSubjectAreaSortedDS') }}
	OUTER JOIN {{ ref('CpyRenameSubjectAreaRejectColumns') }} ON {{ ref('XfmRejectNullLeaseAssetId__OutSubjectAreaSortedDS') }}.FA_CLIENT_UNDERTAKING_ID = {{ ref('CpyRenameSubjectAreaRejectColumns') }}.FA_CLIENT_UNDERTAKING_ID
)

SELECT * FROM Copy_of_JoinSrcSortReject