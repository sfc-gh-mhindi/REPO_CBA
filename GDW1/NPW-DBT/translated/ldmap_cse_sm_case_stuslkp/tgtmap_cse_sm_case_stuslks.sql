{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__sm__case__stus__sm__state__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_SM_CASE_STUSLkp']) }}

SELECT
	SM_STATE_CAT_ID,
	STUS_C 
FROM {{ ref('XfmConversions') }}