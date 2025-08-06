{{ config(materialized='view', tags=['XfmChlBusHlmApp']) }}

WITH 
_cba__app_csel4_dev_dataset_cse__chl__bus__hlmapp__premap AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_cse__chl__bus__hlmapp__premap")  }})
TgtHlmAppPremapDS AS (
	SELECT APP_ID,
		HLM_ACCOUNT_ID,
		ACCOUNT_NUMBER,
		CRIS_PRODUCT_ID,
		HLM_APP_TYPE_CAT_ID,
		DCHG_REAS_ID,
		HL_APP_PROD_ID,
		ORIG_ETL_D
	FROM _cba__app_csel4_dev_dataset_cse__chl__bus__hlmapp__premap
)

SELECT * FROM TgtHlmAppPremapDS