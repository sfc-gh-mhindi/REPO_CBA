{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__pl__acqr__type__pl__cmpn__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_PL_ACQR_TYPELkp']) }}

SELECT
	PL_CMPN_CAT_ID,
	ACQR_TYPE_C 
FROM {{ ref('XfmConversions') }}