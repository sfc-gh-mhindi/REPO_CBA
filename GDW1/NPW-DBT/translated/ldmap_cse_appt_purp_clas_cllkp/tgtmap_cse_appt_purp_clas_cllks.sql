{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__appt__purp__clas__cl__loan__purpose__class__code', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_APPT_PURP_CLAS_CLLkp']) }}

SELECT
	LOAN_PURPOSE_CLASS_CODE,
	PURP_CLAS_C 
FROM {{ ref('XfmConversions') }}