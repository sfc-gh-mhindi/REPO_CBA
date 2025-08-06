{{ config(materialized='view', tags=['LdMAP_CSE_ENV_PATY_RELLkp']) }}

WITH XfmConversions AS (
	SELECT
		{{ ref('SrcMAP_CSE_ENV_PATY_REL_Tera') }}.CLNT_RELN_TYPE_ID AS CLIENT_RELATIONSHIP_TYPE_ID,
		{{ ref('SrcMAP_CSE_ENV_PATY_REL_Tera') }}.CLNT_POSN AS CLIENT_POSITION,
		REL_C
	FROM {{ ref('SrcMAP_CSE_ENV_PATY_REL_Tera') }}
	WHERE 
)

SELECT * FROM XfmConversions