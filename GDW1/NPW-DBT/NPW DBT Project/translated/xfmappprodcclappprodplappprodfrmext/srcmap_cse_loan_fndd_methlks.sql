{{ config(materialized='view', tags=['XfmAppProdCclAppProdPlAppProdFrmExt']) }}

WITH 
_cba__app_pj__gisswrteam_csel4_dev_lookupset_map__cse__loan__fndd__meth__pl__targ__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_pj__gisswrteam_csel4_dev_lookupset_map__cse__loan__fndd__meth__pl__targ__cat__id")  }})
SrcMAP_CSE_LOAN_FNDD_METHLks AS (
	SELECT PL_TARG_CAT_ID,
		LOAN_FNDD_METH_C
	FROM _cba__app_pj__gisswrteam_csel4_dev_lookupset_map__cse__loan__fndd__meth__pl__targ__cat__id
)

SELECT * FROM SrcMAP_CSE_LOAN_FNDD_METHLks