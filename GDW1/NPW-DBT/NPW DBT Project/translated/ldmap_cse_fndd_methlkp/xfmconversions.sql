{{ config(materialized='view', tags=['LdMAP_CSE_FNDD_METHLkp']) }}

WITH XfmConversions AS (
	SELECT
		FNDD_METH_CAT_ID,
		FNDD_INSS_METH_C
	FROM {{ ref('SrcMAP_CSE_FNDD_METHTera') }}
	WHERE 
)

SELECT * FROM XfmConversions