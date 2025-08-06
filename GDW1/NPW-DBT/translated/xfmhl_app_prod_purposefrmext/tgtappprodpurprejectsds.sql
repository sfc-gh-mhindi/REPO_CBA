{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__chl__bus__app__prod__purp__mapping__rejects', incremental_strategy='insert_overwrite', tags=['XfmHL_APP_PROD_PURPOSEFrmExt']) }}

SELECT
	HL_APP_PROD_PURPOSE_ID,
	HL_APP_PROD_ID,
	HL_LOAN_PURPOSE_CAT_ID,
	AMOUNT,
	MAIN_PURPOSE,
	ETL_D,
	ORIG_ETL_D,
	EROR_C 
FROM {{ ref('XfmBusinessRules__OutAppProdPurpRejectsDS') }}