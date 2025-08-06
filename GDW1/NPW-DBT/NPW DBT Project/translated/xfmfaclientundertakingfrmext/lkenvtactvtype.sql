{{ config(materialized='view', tags=['XfmFaClientUndertakingFrmExt']) }}

WITH LkEnvtActvType AS (
	SELECT
		{{ ref('XfmClientPosition') }}.FA_CLIENT_UNDERTAKING_ID,
		{{ ref('XfmClientPosition') }}.FA_UNDERTAKING_ID,
		{{ ref('XfmClientPosition') }}.COIN_ENTITY_ID,
		{{ ref('XfmClientPosition') }}.CLIENT_CORRELATION_ID,
		{{ ref('XfmClientPosition') }}.FA_ENTITY_CAT_ID,
		{{ ref('XfmClientPosition') }}.FA_CHILD_STATUS_CAT_ID,
		{{ ref('XfmClientPosition') }}.CLIENT_RELATIONSHIP_TYPE_ID,
		{{ ref('XfmClientPosition') }}.CLIENT_POSITION,
		{{ ref('XfmClientPosition') }}.IS_PRIMARY_FLAG,
		{{ ref('XfmClientPosition') }}.CIF_CODE,
		{{ ref('XfmClientPosition') }}.ORIG_ETL_D,
		{{ ref('TgtMAP_CSE_ENV_PATY_TYPELks') }}.PATY_TYPE_C,
		{{ ref('TgtMAP_CSE_ENV_PATY_RELLks') }}.REL_C,
		{{ ref('TgtMAP_CSE_ENV_CHILD_PATY_RELLks') }}.REL_C AS REL_C_CHILD
	FROM {{ ref('XfmClientPosition') }}
	LEFT JOIN {{ ref('TgtMAP_CSE_ENV_PATY_TYPELks') }} ON 
	LEFT JOIN {{ ref('TgtMAP_CSE_ENV_PATY_RELLks') }} ON 
	LEFT JOIN {{ ref('TgtMAP_CSE_ENV_CHILD_PATY_RELLks') }} ON 
)

SELECT * FROM LkEnvtActvType