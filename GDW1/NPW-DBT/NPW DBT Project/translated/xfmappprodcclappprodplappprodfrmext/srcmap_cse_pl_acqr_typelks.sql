{{ config(materialized='view', tags=['XfmAppProdCclAppProdPlAppProdFrmExt']) }}

WITH 
_cba__app_pj__gisswrteam_csel4_dev_lookupset_map__cse__pl__acqr__type__pl__cmpn__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_pj__gisswrteam_csel4_dev_lookupset_map__cse__pl__acqr__type__pl__cmpn__cat__id")  }})
SrcMAP_CSE_PL_ACQR_TYPELks AS (
	SELECT PL_CMPN_CAT_ID,
		ACQR_TYPE_C
	FROM _cba__app_pj__gisswrteam_csel4_dev_lookupset_map__cse__pl__acqr__type__pl__cmpn__cat__id
)

SELECT * FROM SrcMAP_CSE_PL_ACQR_TYPELks