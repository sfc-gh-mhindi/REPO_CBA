{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__appt__c__ccl__app__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_APPT_CLkp']) }}

SELECT
	CCL_APP_CAT_ID,
	APPT_C 
FROM {{ ref('XfmConversions') }}