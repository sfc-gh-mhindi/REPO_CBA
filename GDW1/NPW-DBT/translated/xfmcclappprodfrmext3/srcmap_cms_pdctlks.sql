{{ config(materialized='view', tags=['XfmCclAppProdFrmExt3']) }}

WITH 
_cba__app_csel4_dev_lookupset_map__cms__pdct__cris__pdct__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_lookupset_map__cms__pdct__cris__pdct__cat__id")  }})
SrcMAP_CMS_PDCTLks AS (
	SELECT CRIS_PDCT_CAT_ID,
		CRIS_PDCT_C,
		CRIS_DESC,
		ACCT_I_PRFX
	FROM _cba__app_csel4_dev_lookupset_map__cms__pdct__cris__pdct__cat__id
)

SELECT * FROM SrcMAP_CMS_PDCTLks