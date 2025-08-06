{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_cse__chl__bus__hlmapp__merge__bus__hlm__app__tpb__20100614', incremental_strategy='insert_overwrite', tags=['MergeBusHlmAppTpb']) }}

SELECT
	APP_ID,
	HLM_APP_PROD_ID,
	HLM_APP_FOUND_FLAG,
	CHL_TPB_FOUND_FLAG,
	CHL_AGENT_ALIAS_ID,
	CHL_AGENT_NAME,
	HLM_APP__ACCOUNT_ID,
	HLM_APP_ACCOUNT_NUMBER,
	HLM_APP_CRIS_PRODUCT_ID,
	HLM_APP_TYPE_CAT_ID,
	HLM_APP_DISCHARGE_REASON_ID,
	PEXA_FLAG,
	OFI_ID,
	OFI_NAME 
FROM {{ ref('Identify_App_ID_Bus_Hlm') }}