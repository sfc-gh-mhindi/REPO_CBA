{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__sm__case__stus__reas__sm__state__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_SM_CASE_STUS_REASLkp']) }}

SELECT
	SM_REAS_CAT_ID,
	STUS_REAS_TYPE_C 
FROM {{ ref('XfmConversions') }}