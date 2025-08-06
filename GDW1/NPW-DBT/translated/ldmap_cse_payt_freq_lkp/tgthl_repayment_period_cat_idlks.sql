{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__payt__freq__hl__repayment__period__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_PAYT_FREQ_Lkp']) }}

SELECT
	HL_REPAYMENT_PERIOD_CAT_ID,
	PAYT_FREQ_C 
FROM {{ ref('XfmConversions__OutHL_REPAYMENT_PERIOD_CAT_IDLks') }}