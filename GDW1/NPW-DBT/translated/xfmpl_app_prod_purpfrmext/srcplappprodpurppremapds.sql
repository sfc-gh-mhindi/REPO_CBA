{{ config(materialized='view', tags=['XfmPL_APP_PROD_PURPFrmExt']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__app__prod__purp__premap AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__app__prod__purp__premap")  }})
SrcPlAppProdPurpPremapDS AS (
	SELECT PL_APP_PROD_PURP_ID,
		PL_APP_PROD_PURP_CAT_ID,
		AMT,
		PL_APP_PROD_ID,
		ORIG_ETL_D
	FROM _cba__app_csel4_csel4dev_dataset_cse__cpl__bus__app__prod__purp__premap
)

SELECT * FROM SrcPlAppProdPurpPremapDS