{{ config(materialized='view', tags=['XfmCclAppProdFrmExt3']) }}

WITH 
_cba__app_csel4_dev_lookupset_map__cse__pdct__rel__cl__parent__product__level__cat__id__child__product__level__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_lookupset_map__cse__pdct__rel__cl__parent__product__level__cat__id__child__product__level__cat__id")  }})
SrcMAP_CSE_PDCT_REL_CLLks AS (
	SELECT PARENT_PRODUCT_LEVEL_CAT_ID,
		CHILD_PRODUCT_LEVEL_CAT_ID,
		REL_C
	FROM _cba__app_csel4_dev_lookupset_map__cse__pdct__rel__cl__parent__product__level__cat__id__child__product__level__cat__id
)

SELECT * FROM SrcMAP_CSE_PDCT_REL_CLLks