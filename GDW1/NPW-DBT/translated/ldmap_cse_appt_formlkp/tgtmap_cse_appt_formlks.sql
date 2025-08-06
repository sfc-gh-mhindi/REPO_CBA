{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__appt__form__ccl__form__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_APPT_FORMLkp']) }}

SELECT
	CCL_FORM_CAT_ID,
	APPT_FORM_C 
FROM {{ ref('XfmConversions') }}