{{ config(materialized='view', tags=['XfmFaPropClntFrmExt']) }}

WITH CpyRename AS (
	SELECT
		FA_PROPOSED_CLIENT_ID,
		COIN_ENTITY_ID,
		CLIENT_CORRELATION_ID,
		COIN_ENTITY_NAME,
		FA_ENTITY_CAT_ID,
		FA_UNDERTAKING_ID,
		FA_PROPOSED_CLIENT_CAT_ID,
		ORIG_ETL_D,
		change_code
	FROM {{ ref('SrcFAPropClntPremapDS') }}
)

SELECT * FROM CpyRename