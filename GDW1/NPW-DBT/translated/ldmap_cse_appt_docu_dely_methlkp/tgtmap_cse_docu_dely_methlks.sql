{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_lookupset_map__docu__dely__meth', incremental_strategy='insert_overwrite', tags=['LdMAP_CSE_APPT_DOCU_DELY_METHLkp']) }}

SELECT
	TU_DOCCOLLECT_METHOD_CAT_ID,
	DOCU_DELY_METH_C 
FROM {{ ref('XfmConversions') }}