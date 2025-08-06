{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__appt__purp__cl__ccl__loan__purpose__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_APPT_PURP_CLLkp']) }}

SELECT
	CCL_LOAN_PURPOSE_CAT_ID,
	PURP_TYPE_C 
FROM {{ ref('XfmConversions') }}