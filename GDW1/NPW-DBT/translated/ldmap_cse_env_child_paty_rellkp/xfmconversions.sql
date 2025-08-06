{{ config(materialized='view', tags=['LdMAP_CSE_ENV_CHILD_PATY_RELLkp']) }}

WITH XfmConversions AS (
	SELECT
		{{ ref('SrcMAP_CSE_ENV_CHILD_PATY_REL_Tera') }}.FA_CHLD_STAT_CAT_ID AS FA_CHILD_STATUS_CAT_ID,
		REL_C
	FROM {{ ref('SrcMAP_CSE_ENV_CHILD_PATY_REL_Tera') }}
	WHERE 
)

SELECT * FROM XfmConversions