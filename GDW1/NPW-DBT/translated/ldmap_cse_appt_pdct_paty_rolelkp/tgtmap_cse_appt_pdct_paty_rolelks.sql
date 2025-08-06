{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__appt__pdct__paty__role', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_APPT_PDCT_PATY_ROLELkp']) }}

SELECT
	ROLE_CAT_ID,
	PATY_ROLE_C 
FROM {{ ref('XfmConversions') }}