{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__dev_lookupset_map__cse__appt__pdct__feat', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_APPT_PDCT_FEAT_CAMPkp']) }}

SELECT
	CAMPAIGN_CAT_ID,
	FEAT_I,
	ACTL_VAL_R 
FROM {{ ref('XfmConversions') }}