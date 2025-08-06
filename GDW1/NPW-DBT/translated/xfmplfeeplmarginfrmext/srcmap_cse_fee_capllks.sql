{{ config(materialized='view', tags=['XfmPlFeePlMarginFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_map__cse__fee__capl__pl__capl__fee__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_map__cse__fee__capl__pl__capl__fee__cat__id")  }})
SrcMAP_CSE_FEE_CAPLLks AS (
	SELECT PL_CAPL_FEE_CAT_ID,
		FEE_CAPL_F
	FROM _cba__app_csel4_csel4dev_lookupset_map__cse__fee__capl__pl__capl__fee__cat__id
)

SELECT * FROM SrcMAP_CSE_FEE_CAPLLks