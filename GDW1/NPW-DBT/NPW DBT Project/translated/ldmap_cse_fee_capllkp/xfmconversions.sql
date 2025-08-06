{{ config(materialized='view', tags=['LdMAP_CSE_FEE_CAPLLkp']) }}

WITH XfmConversions AS (
	SELECT
		-- *SRC*: Trim(InMAP_CSE_FEE_CAPL_FTera.PL_CAPL_FEE_CAT_ID),
		TRIM({{ ref('SrcMAP_CSE_FEE_CAPL_FTera') }}.PL_CAPL_FEE_CAT_ID) AS PL_CAPL_FEE_CAT_ID,
		-- *SRC*: Trim(InMAP_CSE_FEE_CAPL_FTera.FEE_CAPL_F),
		TRIM({{ ref('SrcMAP_CSE_FEE_CAPL_FTera') }}.FEE_CAPL_F) AS FEE_CAPL_F
	FROM {{ ref('SrcMAP_CSE_FEE_CAPL_FTera') }}
	WHERE 
)

SELECT * FROM XfmConversions