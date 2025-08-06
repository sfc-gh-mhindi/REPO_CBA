{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__payt__freq__repay__frequency__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_PAYT_FREQ_Lkp']) }}

SELECT
	REPAY_FREQUENCY_ID,
	PAYT_FREQ_C 
FROM {{ ref('XfmConversions__OutMAP_CSE_PAYT_FREQLks') }}