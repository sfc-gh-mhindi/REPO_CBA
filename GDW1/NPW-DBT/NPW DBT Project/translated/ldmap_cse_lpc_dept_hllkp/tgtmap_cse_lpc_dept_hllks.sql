{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__lpc__dept__hl__lpc__office', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_LPC_DEPT_HLLkp']) }}

SELECT
	LPC_OFFICE,
	DEPT_I 
FROM {{ ref('XfmConversions') }}