{{ config(materialized='view', tags=['LdTMP_HLM_APP_FrmXfm']) }}

WITH 
_cba__app_csel4_sit_inprocess_cse__chl__bus__hlm__app__cse__chl__bus__hlm__app__20100728 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_sit_inprocess_cse__chl__bus__hlm__app__cse__chl__bus__hlm__app__20100728")  }})
SrcChlBusHlmApp AS (
	SELECT HLM_APP_RECORD_TYPE,
		HLM_APP_MOD_TIMESTAMP,
		HLM_APP_ID,
		HLM_APP__ACCOUNT_ID,
		HLM_APP_ACCOUNT_NUMBER,
		HLM_APP_CRIS_PRODUCT_ID,
		HLM_APP_TYPE_CAT_ID,
		HLM_APP_DISCHARGE_REASON_ID,
		HLM_APP_PROD_ID
	FROM _cba__app_csel4_sit_inprocess_cse__chl__bus__hlm__app__cse__chl__bus__hlm__app__20100728
)

SELECT * FROM SrcChlBusHlmApp