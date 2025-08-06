{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__orig__srce__sys', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_ORIG_SRCE_SYS_CLkp']) }}

SELECT
	ORIG_SRCE_SYST_I,
	ORIG_SRCE_SYST_C 
FROM {{ ref('XfmConversions') }}