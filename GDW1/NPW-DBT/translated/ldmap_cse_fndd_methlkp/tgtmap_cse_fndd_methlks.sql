{{ config(materialized='incremental', alias='_cba__app_hlt_dev_lookupset_map__cse__fndd__meth', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_FNDD_METHLkp']) }}

SELECT
	FNDD_METH_CAT_ID,
	FNDD_INSS_METH_C 
FROM {{ ref('XfmConversions') }}