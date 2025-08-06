{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__appt__orig__chnl__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_APPT_CHNLLkp']) }}

SELECT
	CHNL_CAT_ID,
	APPT_ORIG_C 
FROM {{ ref('XfmConversions') }}