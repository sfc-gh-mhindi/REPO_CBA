{{ config(materialized='view', tags=['LdREJT_CHL_BUS_HLM_APP']) }}

WITH 
_cba__app_csel4_dev_dataset_cse__chl__bus__hlm__ap__mapping__rejects AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_cse__chl__bus__hlm__ap__mapping__rejects")  }})
SrcChlBusHlmAppRejectsDS AS (
	SELECT HLM_APP_ID,
		HLM_ACCOUNT_ID,
		ACCOUNT_NUMBER,
		CRIS_PRODUCT_ID,
		HLM_APP_TYPE_CAT_ID,
		DISCHARGE_REASON_ID,
		HL_APP_PROD_ID,
		ETL_D,
		ORIG_ETL_D,
		EROR_C
	FROM _cba__app_csel4_dev_dataset_cse__chl__bus__hlm__ap__mapping__rejects
)

SELECT * FROM SrcChlBusHlmAppRejectsDS