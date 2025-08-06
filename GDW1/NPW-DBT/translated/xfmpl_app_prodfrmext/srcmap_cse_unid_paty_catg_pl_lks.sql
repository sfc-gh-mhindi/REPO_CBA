{{ config(materialized='view', tags=['XfmPL_APP_PRODFrmExt']) }}

WITH 
_cba__app_csel4_dev_lookupset_map__cse__unid__paty__catg__pl__tp__broker__group__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_lookupset_map__cse__unid__paty__catg__pl__tp__broker__group__cat__id")  }})
SrcMAP_CSE_UNID_PATY_CATG_PL_Lks AS (
	SELECT TP_BROKER_GROUP_CAT_ID,
		UNID_PATY_CATG_C
	FROM _cba__app_csel4_dev_lookupset_map__cse__unid__paty__catg__pl__tp__broker__group__cat__id
)

SELECT * FROM SrcMAP_CSE_UNID_PATY_CATG_PL_Lks