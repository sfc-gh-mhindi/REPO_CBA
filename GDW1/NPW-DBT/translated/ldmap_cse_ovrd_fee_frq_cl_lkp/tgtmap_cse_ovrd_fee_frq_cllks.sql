{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__ovrd__fee__frq__cl__override__fee__pct__freq', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_OVRD_FEE_FRQ_CL_Lkp']) }}

SELECT
	OVRD_FEE_PCT_FREQ,
	FREQ_IN_MTHS 
FROM {{ ref('XfmConversions') }}