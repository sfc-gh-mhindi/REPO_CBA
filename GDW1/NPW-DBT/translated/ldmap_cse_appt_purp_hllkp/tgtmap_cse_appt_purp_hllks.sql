{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__appt__purp__hl__hl__loan__purp__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_APPT_PURP_HLLkp']) }}

SELECT
	HL_LOAN_PURPOSE_CAT_ID,
	PURP_TYPE_C 
FROM {{ ref('XfmConversions') }}