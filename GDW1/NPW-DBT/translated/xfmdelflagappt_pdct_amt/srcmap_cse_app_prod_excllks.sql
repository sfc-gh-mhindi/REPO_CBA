{{ config(materialized='view', tags=['XfmDelFlagAPPT_PDCT_AMT']) }}

WITH 
_cba__app_csel4_csel4dev_lookupset_map__cse__app__prod__excl__app__prod__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_lookupset_map__cse__app__prod__excl__app__prod__id")  }})
SrcMAP_CSE_APP_PROD_EXCLLks AS (
	SELECT APP_PROD_ID,
		DUMMY_PDCT_F
	FROM _cba__app_csel4_csel4dev_lookupset_map__cse__app__prod__excl__app__prod__id
)

SELECT * FROM SrcMAP_CSE_APP_PROD_EXCLLks