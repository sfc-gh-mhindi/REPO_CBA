{{ config(materialized='view', tags=['XfmTu_ApptPdctFnddInssFrmExt']) }}

WITH 
_cba__app_hlt_dev_lookupset_map__cse__fndd__meth AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_dev_lookupset_map__cse__fndd__meth")  }})
TgtMAP_CSE_FNDD_METHLks AS (
	SELECT FNDD_METH_CAT_ID,
		FNDD_INSS_METH_C
	FROM _cba__app_hlt_dev_lookupset_map__cse__fndd__meth
)

SELECT * FROM TgtMAP_CSE_FNDD_METHLks