{{ config(materialized='view', tags=['XfmChlBusHlmAppSecFrmExt']) }}

WITH 
_cba__app_csel4_dev_dataset_cse__chl__bus__hlm__app__sec__premap AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_cse__chl__bus__hlm__app__sec__premap")  }})
SrcChlBusHlmAppSecDS AS (
	SELECT HL_APP_PROD_ID,
		HLM_APP_ID,
		ASSET_LIABILITY_ID,
		PRINCIPAL_SECU_FLAG,
		SETTLEMENT_REQD,
		FORWARD_DOC_TO,
		TO_MS,
		TO_BRANCH,
		TO_AGENT,
		SETTLEMENT_COMMENT
	FROM _cba__app_csel4_dev_dataset_cse__chl__bus__hlm__app__sec__premap
)

SELECT * FROM SrcChlBusHlmAppSecDS