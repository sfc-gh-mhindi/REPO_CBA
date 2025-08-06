{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__env__evnt__actv__type__fa__env__evnt__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_ENV_EVNT_ACT_TYPELkp']) }}

SELECT
	FA_ENV_EVNT_CAT_ID,
	EVNT_ACTV_TYPE_C 
FROM {{ ref('XfmConversions') }}