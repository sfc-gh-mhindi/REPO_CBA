{{ config(materialized='view', tags=['LdREJT_CPL_BUS_APP_PROD_PURP']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__app__prod__purp__plbusappprodpurp__nulls__rejects AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__app__prod__purp__plbusappprodpurp__nulls__rejects")  }})
RejCplBusAppProdPurpDS AS (
	SELECT PL_APP_PROD_PURP_ID,
		PL_APP_PROD_PURP_CAT_ID,
		AMT,
		PL_APP_PROD_ID,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM _cba__app_csel4_csel4dev_dataset_cse__cpl__bus__app__prod__purp__plbusappprodpurp__nulls__rejects
)

SELECT * FROM RejCplBusAppProdPurpDS