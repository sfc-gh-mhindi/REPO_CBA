{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__pack__pdct__hl__hl__pack__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_PACK_PDCT_HLLkp']) }}

SELECT
	HL_PACKAGE_CAT_ID,
	PDCT_N 
FROM {{ ref('XfmConversions') }}