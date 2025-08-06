{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__cris__pdct', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_CRIS_PDCTLkp']) }}

SELECT
	CRIS_PDCT_C,
	ACCT_QLFY_C,
	SRCE_SYST_C 
FROM {{ ref('XfmConversions') }}