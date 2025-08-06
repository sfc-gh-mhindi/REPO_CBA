{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__cnty__code', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_CNTY_CODELkp']) }}

SELECT
	CNTY_ID,
	ISO_CNTY_C 
FROM {{ ref('XfmConversions') }}