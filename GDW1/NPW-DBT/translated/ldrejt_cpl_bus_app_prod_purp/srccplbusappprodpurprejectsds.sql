{{ config(materialized='view', tags=['LdREJT_CPL_BUS_APP_PROD_PURP']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__app__prod__purp__mapping__rejects AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__app__prod__purp__mapping__rejects")  }})
SrcCplBusAppProdPurpRejectsDS AS (
	SELECT PL_APP_PROD_PURP_ID,
		PL_APP_PROD_PURP_CAT_ID,
		AMT,
		PL_APP_PROD_ID,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM _cba__app_csel4_csel4dev_dataset_cse__cpl__bus__app__prod__purp__mapping__rejects
)

SELECT * FROM SrcCplBusAppProdPurpRejectsDS