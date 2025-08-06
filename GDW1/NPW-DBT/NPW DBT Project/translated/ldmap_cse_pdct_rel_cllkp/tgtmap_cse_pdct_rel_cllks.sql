{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__pdct__rel__cl__parent__product__level__cat__id__child__product__level__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_PDCT_REL_CLLkp']) }}

SELECT
	PARENT_PRODUCT_LEVEL_CAT_ID,
	CHILD_PRODUCT_LEVEL_CAT_ID,
	REL_C 
FROM {{ ref('XfmConversions') }}