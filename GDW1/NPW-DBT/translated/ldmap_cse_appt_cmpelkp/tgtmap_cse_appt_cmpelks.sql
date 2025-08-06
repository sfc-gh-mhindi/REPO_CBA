{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__appt__cmpe__bal__xfer__institution__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_APPT_CMPELkp']) }}

SELECT
	BAL_XFER_INSTITUTION_CAT_ID,
	CMPE_I 
FROM {{ ref('XfmConversions') }}