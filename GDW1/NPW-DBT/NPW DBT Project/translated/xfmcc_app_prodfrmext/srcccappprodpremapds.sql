{{ config(materialized='view', tags=['XfmCC_APP_PRODFrmExt']) }}

WITH 
_cba__app_csel4_dev_dataset_cse__ccc__bus__app__prod__premap AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_cse__ccc__bus__app__prod__premap")  }})
SrcCCAppProdPremapDS AS (
	SELECT CC_APP_PROD_ID,
		REQUESTED_LIMIT_AMT,
		CC_INTEREST_OPT_CAT_ID,
		CBA_HOMELOAN_NO,
		PRE_APPRV_AMOUNT,
		ORIG_ETL_D,
		READ_COSTS_AND_RISKS_FLAG,
		ACCEPTS_COSTS_AND_RISKS_DATE,
		NTM_CAMPAIGN_ID,
		FRIES_CAMPAIGN_CODE,
		OAP_CAMPAIGN_CODE
	FROM _cba__app_csel4_dev_dataset_cse__ccc__bus__app__prod__premap
)

SELECT * FROM SrcCCAppProdPremapDS