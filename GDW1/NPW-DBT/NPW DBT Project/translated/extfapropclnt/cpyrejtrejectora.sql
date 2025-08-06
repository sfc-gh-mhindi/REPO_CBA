{{ config(materialized='view', tags=['ExtFaPropClnt']) }}

WITH CpyRejtRejectOra AS (
	SELECT
		FA_PROPOSED_CLIENT_ID,
		{{ ref('SrcRejtRejectOra') }}.COIN_ENTITY_ID AS COIN_ENTITY_ID_R,
		{{ ref('SrcRejtRejectOra') }}.CLIENT_CORRELATION_ID AS CLIENT_CORRELATION_ID_R,
		{{ ref('SrcRejtRejectOra') }}.COIN_ENTITY_NAME AS COIN_ENTITY_NAME_R,
		{{ ref('SrcRejtRejectOra') }}.FA_ENTITY_CAT_ID AS FA_ENTITY_CAT_ID_R,
		FA_UNDERTAKING_ID,
		{{ ref('SrcRejtRejectOra') }}.FA_PROPOSED_CLIENT_CAT_ID AS FA_PROPOSED_CLIENT_CAT_ID_R,
		{{ ref('SrcRejtRejectOra') }}.ORIG_ETL_D AS ORIG_ETL_D_R
	FROM {{ ref('SrcRejtRejectOra') }}
)

SELECT * FROM CpyRejtRejectOra