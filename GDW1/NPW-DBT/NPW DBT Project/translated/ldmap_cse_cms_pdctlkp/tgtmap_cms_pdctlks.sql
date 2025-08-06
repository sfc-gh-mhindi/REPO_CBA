{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cms__pdct__cris__pdct__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_CMS_PDCTLkp']) }}

SELECT
	CRIS_PDCT_CAT_ID,
	CRIS_PDCT_C,
	CRIS_DESC,
	ACCT_I_PRFX 
FROM {{ ref('XfmConversions') }}