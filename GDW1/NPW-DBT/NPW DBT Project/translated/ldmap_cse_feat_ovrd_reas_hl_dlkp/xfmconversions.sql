{{ config(materialized='view', tags=['LdMAP_CSE_FEAT_OVRD_REAS_HL_DLkp']) }}

WITH XfmConversions AS (
	SELECT
		HL_FEE_DISCOUNT_CAT_ID,
		OVRD_REAS_C
	FROM {{ ref('SrcMAP_CSE_FEAT_OVRD_REAS_HL_D') }}
	WHERE 
)

SELECT * FROM XfmConversions