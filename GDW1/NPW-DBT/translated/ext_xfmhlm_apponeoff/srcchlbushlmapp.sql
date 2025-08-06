{{ config(materialized='view', tags=['Ext_XfmHLM_APPONEOFF']) }}

WITH 
_cba__app_csel4_dev_inprocess_one__off__load__pexa__bus__hlm__app__cse__chl__bus__hlmapp__20100614 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_inprocess_one__off__load__pexa__bus__hlm__app__cse__chl__bus__hlmapp__20100614")  }})
SrcChlBusHlmApp AS (
	SELECT HLM_APP_RECORD_TYPE,
		HLM_APP_MOD_TIMESTAMP,
		HLM_APP_ID,
		HLM_APP__ACCOUNT_ID,
		HLM_APP_ACCOUNT_NUMBER,
		HLM_APP_CRIS_PRODUCT_ID,
		HLM_APP_TYPE_CAT_ID,
		HLM_APP_DISCHARGE_REASON_ID,
		HLM_APP_PROD_ID,
		DISCHARGE_EXTERNAL_OFI_ID,
		INSTITUTION_NAME,
		PEXA_FLAG
	FROM _cba__app_csel4_dev_inprocess_one__off__load__pexa__bus__hlm__app__cse__chl__bus__hlmapp__20100614
)

SELECT * FROM SrcChlBusHlmApp