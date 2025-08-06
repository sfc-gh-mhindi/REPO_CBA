{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__feat__ovrd__reas__hl__prod__int__margin__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_FEAT_OVRD_REAS_HLLkp']) }}

SELECT
	HL_PROD_INT_MARGIN_CAT_ID,
	FEAT_OVRD_REAS_C 
FROM {{ ref('XfmConversions') }}