{{ config(materialized='view', tags=['XfmChlBusFeeDiscFeeFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_map__cse__feat__ovrd__reas__hl__d AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_map__cse__feat__ovrd__reas__hl__d")  }})
SrcMAP_CSE_FEAT_OVRD_REAS_HL_D AS (
	SELECT HL_FEE_DISCOUNT_CAT_ID,
		OVRD_REAS_C
	FROM _cba__app_csel4_csel4dev_lookupset_map__cse__feat__ovrd__reas__hl__d
)

SELECT * FROM SrcMAP_CSE_FEAT_OVRD_REAS_HL_D