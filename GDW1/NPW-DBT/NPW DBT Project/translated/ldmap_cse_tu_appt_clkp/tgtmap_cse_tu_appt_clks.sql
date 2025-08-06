{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__tu__appt__c', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_TU_APPT_CLkp']) }}

SELECT
	SBTY_CODE,
	APPT_C 
FROM {{ ref('XfmConversions__OutMAP_CSE_TU_APPT_CLks') }}