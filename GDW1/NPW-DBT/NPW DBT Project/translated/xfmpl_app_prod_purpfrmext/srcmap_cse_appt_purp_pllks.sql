{{ config(materialized='view', tags=['XfmPL_APP_PROD_PURPFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_map__cse__appt__purp__pl__pl__app__prod__purp__cat__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_map__cse__appt__purp__pl__pl__app__prod__purp__cat__id")  }})
SrcMAP_CSE_APPT_PURP_PLLks AS (
	SELECT PL_APP_PROD_PURP_CAT_ID,
		PURP_TYPE_C
	FROM _cba__app_csel4_csel4dev_lookupset_map__cse__appt__purp__pl__pl__app__prod__purp__cat__id
)

SELECT * FROM SrcMAP_CSE_APPT_PURP_PLLks