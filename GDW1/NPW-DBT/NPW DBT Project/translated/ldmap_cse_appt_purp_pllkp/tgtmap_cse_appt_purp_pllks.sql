{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__appt__purp__pl__pl__app__prod__purp__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_APPT_PURP_PLLkp']) }}

SELECT
	PL_APP_PROD_PURP_CAT_ID,
	PURP_TYPE_C 
FROM {{ ref('XfmConversions') }}