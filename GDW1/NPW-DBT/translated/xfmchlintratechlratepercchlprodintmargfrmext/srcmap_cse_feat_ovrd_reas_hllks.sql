{{ config(materialized='view', tags=['XfmChlIntRateChlRatePercChlProdIntMargFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_map__cse__feat__ovrd__reas__hl__prod__int__margin__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_map__cse__feat__ovrd__reas__hl__prod__int__margin__cat__id")  }})
SrcMAP_CSE_FEAT_OVRD_REAS_HLLks AS (
	SELECT HL_PROD_INT_MARGIN_CAT_ID,
		FEAT_OVRD_REAS_C
	FROM _cba__app_csel4_csel4dev_lookupset_map__cse__feat__ovrd__reas__hl__prod__int__margin__cat__id
)

SELECT * FROM SrcMAP_CSE_FEAT_OVRD_REAS_HLLks