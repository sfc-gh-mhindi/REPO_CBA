{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__ccl__bus__app__cclapp__nulls__rejects', incremental_strategy='insert_overwrite', tags=['ExtCclBusApp']) }}

SELECT
	CCL_APP_ID,
	CCL_APP_CAT_ID,
	CCL_FORM_CAT_ID,
	TOTAL_PERSONAL_FAC_AMT,
	TOTAL_EQUIPMENTFINANCE_FAC_AMT,
	TOTAL_COMMERCIAL_FAC_AMT,
	TOPUP_APP_ID,
	AF_PRIMARY_INDUSTRY_ID,
	AD_TUC_AMT,
	COMMISSION_AMT,
	BROKER_REFERAL_FLAG,
	ETL_D,
	ORIG_ETL_D,
	EROR_C 
FROM {{ ref('XfmCheckCclAppIdNulls__OutCclAppIdNullsDS') }}