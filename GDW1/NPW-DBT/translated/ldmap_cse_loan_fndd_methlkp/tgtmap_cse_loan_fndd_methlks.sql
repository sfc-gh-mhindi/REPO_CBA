{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__loan__fndd__meth__pl__targ__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_LOAN_FNDD_METHLkp']) }}

SELECT
	PL_TARG_CAT_ID,
	LOAN_FNDD_METH_C 
FROM {{ ref('XfmConversions') }}