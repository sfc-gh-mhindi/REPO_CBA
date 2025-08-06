{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__env__paty__type__fa__entity__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_ENV_PATY_TYPELkp']) }}

SELECT
	FA_ENTITY_CAT_ID,
	PATY_TYPE_C 
FROM {{ ref('XfmConversions') }}