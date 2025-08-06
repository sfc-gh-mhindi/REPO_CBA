{{ config(materialized='view', tags=['LdMAP_CSE_PACK_PDCT_HLLkp']) }}

WITH XfmConversions AS (
	SELECT
		-- *SRC*: Trim(InMAP_CSE_PACK_PDCT_HLTera.HL_PACK_CAT_ID),
		TRIM({{ ref('SrcMAP_CSE_PACK_PDCT_HLTera') }}.HL_PACK_CAT_ID) AS HL_PACKAGE_CAT_ID,
		-- *SRC*: Trim(InMAP_CSE_PACK_PDCT_HLTera.PDCT_N),
		TRIM({{ ref('SrcMAP_CSE_PACK_PDCT_HLTera') }}.PDCT_N) AS PDCT_N
	FROM {{ ref('SrcMAP_CSE_PACK_PDCT_HLTera') }}
	WHERE 
)

SELECT * FROM XfmConversions