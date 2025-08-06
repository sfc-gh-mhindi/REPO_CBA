{{ config(materialized='view', tags=['LdMAP_CSE_PACK_PDCT_PLLkp']) }}

WITH XfmConversions AS (
	SELECT
		-- *SRC*: Trim(InMAP_CSE_PACK_PDCT_PLTera.PL_PACK_CAT_ID),
		TRIM({{ ref('SrcMAP_CSE_PACK_PDCT_PLTera') }}.PL_PACK_CAT_ID) AS PL_PACKAGE_CAT_ID,
		-- *SRC*: Trim(InMAP_CSE_PACK_PDCT_PLTera.PDCT_N),
		TRIM({{ ref('SrcMAP_CSE_PACK_PDCT_PLTera') }}.PDCT_N) AS PDCT_N
	FROM {{ ref('SrcMAP_CSE_PACK_PDCT_PLTera') }}
	WHERE 
)

SELECT * FROM XfmConversions