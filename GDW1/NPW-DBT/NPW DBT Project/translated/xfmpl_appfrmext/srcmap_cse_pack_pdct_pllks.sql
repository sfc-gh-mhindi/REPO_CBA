{{ config(materialized='view', tags=['XfmPL_APPFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_map__cse__pack__pdct__pl__pl__pack__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_map__cse__pack__pdct__pl__pl__pack__cat__id")  }})
SrcMAP_CSE_PACK_PDCT_PLLks AS (
	SELECT PL_PACKAGE_CAT_ID,
		PDCT_N
	FROM _cba__app_csel4_csel4dev_lookupset_map__cse__pack__pdct__pl__pl__pack__cat__id
)

SELECT * FROM SrcMAP_CSE_PACK_PDCT_PLLks