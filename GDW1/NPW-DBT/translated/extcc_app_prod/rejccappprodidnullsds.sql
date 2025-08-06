{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_cse__ccc__bus__app__prod__ccappprod__nulls__rejects', incremental_strategy='insert_overwrite', tags=['ExtCC_APP_PROD']) }}

SELECT
	CC_APP_PROD_ID,
	REQUESTED_LIMIT_AMT,
	CC_INTEREST_OPT_CAT_ID,
	CBA_HOMELOAN_NO,
	PRE_APPRV_AMOUNT,
	ETL_D,
	ORIG_ETL_D,
	EROR_C,
	READ_COSTS_AND_RISKS_FLAG,
	ACCEPTS_COSTS_AND_RISKS_DATE 
FROM {{ ref('XfmCheckCCAppProdIdNulls__OutCCAppProdIdNullsDS') }}