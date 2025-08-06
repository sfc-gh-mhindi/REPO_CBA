{{ config(materialized='view', tags=['LdMAP_CSE_STATELkp']) }}

WITH XfmConversions AS (
	SELECT
		{{ ref('SrcMAP_CSE_STATETera') }}.STATE_ID AS STAT_C,
		STAT_X
	FROM {{ ref('SrcMAP_CSE_STATETera') }}
	WHERE 
)

SELECT * FROM XfmConversions