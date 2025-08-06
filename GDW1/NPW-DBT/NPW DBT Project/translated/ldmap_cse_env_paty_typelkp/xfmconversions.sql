{{ config(materialized='view', tags=['LdMAP_CSE_ENV_PATY_TYPELkp']) }}

WITH XfmConversions AS (
	SELECT
		-- *SRC*: trim(InMAP_CSE_ENV_PATY_TYPETera.FA_ENTY_CAT_ID),
		TRIM({{ ref('SrcMAP_CSE_ENV_PATY_TYPE_Tera') }}.FA_ENTY_CAT_ID) AS FA_ENTITY_CAT_ID,
		PATY_TYPE_C
	FROM {{ ref('SrcMAP_CSE_ENV_PATY_TYPE_Tera') }}
	WHERE 
)

SELECT * FROM XfmConversions