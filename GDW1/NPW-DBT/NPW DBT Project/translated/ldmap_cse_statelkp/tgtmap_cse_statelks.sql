{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__state', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_STATELkp']) }}

SELECT
	STAT_C,
	STAT_X 
FROM {{ ref('XfmConversions') }}