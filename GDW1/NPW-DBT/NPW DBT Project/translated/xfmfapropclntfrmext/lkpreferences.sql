{{ config(materialized='view', tags=['XfmFaPropClntFrmExt']) }}

WITH LkpReferences AS (
	SELECT
		{{ ref('CpyRename') }}.FA_PROPOSED_CLIENT_ID,
		{{ ref('CpyRename') }}.COIN_ENTITY_ID,
		{{ ref('CpyRename') }}.CLIENT_CORRELATION_ID,
		{{ ref('CpyRename') }}.COIN_ENTITY_NAME,
		{{ ref('CpyRename') }}.FA_ENTITY_CAT_ID,
		{{ ref('CpyRename') }}.FA_UNDERTAKING_ID,
		{{ ref('CpyRename') }}.FA_PROPOSED_CLIENT_CAT_ID,
		{{ ref('CpyRename') }}.ORIG_ETL_D,
		{{ ref('CpyRename') }}.change_code,
		{{ ref('SrcMAP_CSE_ENV_PATY_TYPELks') }}.PATY_TYPE_C
	FROM {{ ref('CpyRename') }}
	LEFT JOIN {{ ref('SrcMAP_CSE_ENV_PATY_TYPELks') }} ON 
)

SELECT * FROM LkpReferences