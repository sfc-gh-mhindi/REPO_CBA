{{ config(materialized='view', tags=['XfmFaClientUndertakingFrmExt']) }}

WITH XfmClientPosition AS (
	SELECT
		FA_CLIENT_UNDERTAKING_ID,
		FA_UNDERTAKING_ID,
		COIN_ENTITY_ID,
		CLIENT_CORRELATION_ID,
		FA_ENTITY_CAT_ID,
		FA_CHILD_STATUS_CAT_ID,
		CLIENT_RELATIONSHIP_TYPE_ID,
		-- *SRC*: \(20)If Len(Trim(( IF IsNotNull((OutFAclientUndertakingDS.CLIENT_POSITION)) THEN (OutFAclientUndertakingDS.CLIENT_POSITION) ELSE ""))) = 0 then '1' else OutFAclientUndertakingDS.CLIENT_POSITION,
		IFF(LEN(TRIM(IFF({{ ref('SrcFAClientundertakingDS') }}.CLIENT_POSITION IS NOT NULL, {{ ref('SrcFAClientundertakingDS') }}.CLIENT_POSITION, ''))) = 0, '1', {{ ref('SrcFAClientundertakingDS') }}.CLIENT_POSITION) AS CLIENT_POSITION,
		IS_PRIMARY_FLAG,
		CIF_CODE,
		ORIG_ETL_D
	FROM {{ ref('SrcFAClientundertakingDS') }}
	WHERE 
)

SELECT * FROM XfmClientPosition