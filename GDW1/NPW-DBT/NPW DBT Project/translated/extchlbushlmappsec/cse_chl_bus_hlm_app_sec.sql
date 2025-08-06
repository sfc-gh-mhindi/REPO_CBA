{{ config(materialized='view', tags=['ExtChlBusHlmAppSec']) }}

WITH 
_cba__app_csel4_dev_inprocess_cse__chl__bus__hlm__app__sec__cse__chl__bus__hlm__app__sec__20100616 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_inprocess_cse__chl__bus__hlm__app__sec__cse__chl__bus__hlm__app__sec__20100616")  }})
CSE_CHL_BUS_HLM_APP_SEC AS (
	SELECT REC_TYPE,
		MOD_TIMESTAMP,
		HL_APP_PROD_ID,
		HLM_APP_ID,
		HL_APP_SECURITY_ID,
		ASSET_LIABILITY_ID,
		PRINCIPAL_SECU_FLAG,
		SETTLEMENT_REQD,
		SETTLE_AT_OFFICE,
		FORWARD_DOC_TO,
		TO_MS,
		TO_BRANCH,
		TO_AGENT,
		SETTLEMENT_COMMENT
	FROM _cba__app_csel4_dev_inprocess_cse__chl__bus__hlm__app__sec__cse__chl__bus__hlm__app__sec__20100616
)

SELECT * FROM CSE_CHL_BUS_HLM_APP_SEC