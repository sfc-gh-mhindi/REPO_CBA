{{ config(materialized='view', tags=['LdREJT_CHL_BUS_APP_PROD_PURP']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_cse__chl__bus__app__prod__purp__chlbusappprodpurpose__nulls__rejects AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_cse__chl__bus__app__prod__purp__chlbusappprodpurpose__nulls__rejects")  }})
RejChlBusAppProdPurpDS AS (
	SELECT HL_APP_PROD_PURPOSE_ID,
		HL_APP_PROD_ID,
		HL_LOAN_PURPOSE_CAT_ID,
		AMOUNT,
		MAIN_PURPOSE,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM _cba__app_csel4_csel4dev_dataset_cse__chl__bus__app__prod__purp__chlbusappprodpurpose__nulls__rejects
)

SELECT * FROM RejChlBusAppProdPurpDS