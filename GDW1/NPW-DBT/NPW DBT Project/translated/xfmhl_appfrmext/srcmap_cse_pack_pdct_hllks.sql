{{ config(materialized='view', tags=['XfmHL_APPFrmExt']) }}

WITH 
_cba__app_pj__rapidresponseteam_csel4_dev_lookupset_map__cse__pack__pdct__hl__hl__pack__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_pj__rapidresponseteam_csel4_dev_lookupset_map__cse__pack__pdct__hl__hl__pack__cat__id")  }})
SrcMAP_CSE_PACK_PDCT_HLLks AS (
	SELECT HL_PACKAGE_CAT_ID,
		PDCT_N
	FROM _cba__app_pj__rapidresponseteam_csel4_dev_lookupset_map__cse__pack__pdct__hl__hl__pack__cat__id
)

SELECT * FROM SrcMAP_CSE_PACK_PDCT_HLLks