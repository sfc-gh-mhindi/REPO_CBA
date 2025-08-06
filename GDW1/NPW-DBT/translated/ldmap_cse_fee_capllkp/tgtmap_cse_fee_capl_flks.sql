{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__fee__capl__pl__capl__fee__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_FEE_CAPLLkp']) }}

SELECT
	PL_CAPL_FEE_CAT_ID,
	FEE_CAPL_F 
FROM {{ ref('XfmConversions') }}