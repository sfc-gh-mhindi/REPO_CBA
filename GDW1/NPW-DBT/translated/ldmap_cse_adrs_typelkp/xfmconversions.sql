{{ config(materialized='view', tags=['LdMAP_CSE_ADRS_TYPELkp']) }}

WITH XfmConversions AS (
	SELECT
		ADRS_TYPE_ID,
		PYAD_TYPE_C
	FROM {{ ref('SrcMAP_CSE_ADRS_TYPETera') }}
	WHERE 
)

SELECT * FROM XfmConversions