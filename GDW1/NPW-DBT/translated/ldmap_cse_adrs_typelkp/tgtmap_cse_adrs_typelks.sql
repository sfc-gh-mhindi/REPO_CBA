{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__adrs__type', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_ADRS_TYPELkp']) }}

SELECT
	ADRS_TYPE_ID,
	PYAD_TYPE_C 
FROM {{ ref('XfmConversions') }}