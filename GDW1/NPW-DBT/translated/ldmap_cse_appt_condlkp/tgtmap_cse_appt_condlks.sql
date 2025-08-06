{{ config(materialized='incremental', alias='_cba__app_hlt_dev_lookupset_map__cse__appt__cond', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_APPT_CONDLkp']) }}

SELECT
	COND_APPT_CAT_ID,
	APPT_COND_C 
FROM {{ ref('XfmConversions') }}