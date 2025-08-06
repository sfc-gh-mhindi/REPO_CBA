{{ config(materialized='view', tags=['ExtAppProdCclAppProdPlAppProd']) }}

WITH 
_cba__app_csel4_dev_lookupset_map__cse__app__prod__excl__app__prod__id AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_lookupset_map__cse__app__prod__excl__app__prod__id")  }})
SrcAPP_PROD_EXCLLks AS (
	SELECT APP_PROD_ID,
		DUMMY_PDCT_F
	FROM _cba__app_csel4_dev_lookupset_map__cse__app__prod__excl__app__prod__id
)

SELECT * FROM SrcAPP_PROD_EXCLLks