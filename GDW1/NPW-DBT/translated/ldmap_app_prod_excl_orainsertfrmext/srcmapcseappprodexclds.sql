{{ config(materialized='view', tags=['LdMAP_APP_PROD_EXCL_ORAInsertFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__com__bus__app__prod__ccl__pl__app__prod__mapcseappprodexcl__insert AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__com__bus__app__prod__ccl__pl__app__prod__mapcseappprodexcl__insert")  }})
SrcMapCseAppProdExclDS AS (
	SELECT APP_PROD_ID,
		DUMMY_PDCT_F,
		CRAT_D
	FROM _cba__app_csel4_csel4dev_dataset_cse__com__bus__app__prod__ccl__pl__app__prod__mapcseappprodexcl__insert
)

SELECT * FROM SrcMapCseAppProdExclDS