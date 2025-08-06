{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__cse__trnf__optn__bal__xfer__option__cat__id', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_TRNF_OPTNLkp']) }}

SELECT
	BAL_XFER_OPTION_CAT_ID,
	TRNF_OPTN_C 
FROM {{ ref('XfmConversions') }}