{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_cse__chl__bus__hlm__app__sec__premap', incremental_strategy='insert_overwrite', tags=['ExtChlBusHlmAppSec']) }}

SELECT
	HL_APP_PROD_ID,
	HLM_APP_ID,
	ASSET_LIABILITY_ID,
	PRINCIPAL_SECU_FLAG,
	SETTLEMENT_REQD,
	FORWARD_DOC_TO,
	TO_MS,
	TO_BRANCH,
	TO_AGENT,
	SETTLEMENT_COMMENT 
FROM {{ ref('XfmCheckNulls') }}