{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__env__child__paty__rel__fa__child__status__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_ENV_CHILD_PATY_RELLkp']) }}

SELECT
	FA_CHILD_STATUS_CAT_ID,
	REL_C 
FROM {{ ref('XfmConversions') }}