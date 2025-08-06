{{ config(materialized='incremental', alias='_cba__app_mme_dev_dataset_cse__chl__bus__app__prod__mapping__rejects', incremental_strategy='insert_overwrite', tags=['XfmHL_APP_PRODFrmExt']) }}

SELECT
	HL_APP_PROD_ID,
	PARENT_HL_APP_PROD_ID,
	HL_REPAYMENT_PERIOD_CAT_ID,
	AMOUNT,
	LOAN_TERM_MONTHS,
	ACCOUNT_NUMBER,
	TOTAL_LOAN_AMOUNT,
	HLS_FLAG,
	GDW_UPDATED_LDP_PAID_ON_AMOUNT,
	ETL_D,
	ORIG_ETL_D,
	EROR_C 
FROM {{ ref('XfmBusinessRules__OutHlAppProdRejectsDS') }}