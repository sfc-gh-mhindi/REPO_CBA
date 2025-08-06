{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__feat__ovrd__reas__hl__d', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_FEAT_OVRD_REAS_HL_DLkp']) }}

SELECT
	HL_FEE_DISCOUNT_CAT_ID,
	OVRD_REAS_C 
FROM {{ ref('XfmConversions') }}