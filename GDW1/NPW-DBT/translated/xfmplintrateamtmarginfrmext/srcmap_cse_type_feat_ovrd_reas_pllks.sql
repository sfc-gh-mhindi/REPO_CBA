{{ config(materialized='view', tags=['XfmPlIntRateAmtMarginFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_map__cse__feat__ovrd__reas__pl__marg__reas__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_map__cse__feat__ovrd__reas__pl__marg__reas__cat__id")  }})
SrcMAP_CSE_TYPE_FEAT_OVRD_REAS_PLLks AS (
	SELECT MARG_REAS_CAT_ID,
		FEAT_OVRD_REAS_C
	FROM _cba__app_csel4_csel4dev_lookupset_map__cse__feat__ovrd__reas__pl__marg__reas__cat__id
)

SELECT * FROM SrcMAP_CSE_TYPE_FEAT_OVRD_REAS_PLLks