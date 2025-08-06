{{ config(materialized='incremental', alias='_cba__app_lpxs_lpxs__sit_lookupset_map__cse__appt__qstn ', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_APPT_QSTNLkp']) }}

SELECT
	QSTN_ID,
	ROW_S 
FROM {{ ref('XfmConversions') }}