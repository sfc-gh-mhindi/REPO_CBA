{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__app__prod__purp__mapping__rejects', incremental_strategy='insert_overwrite', tags=['XfmPL_APP_PROD_PURPFrmExt']) }}

SELECT
	PL_APP_PROD_PURP_ID,
	PL_APP_PROD_PURP_CAT_ID,
	AMT,
	PL_APP_PROD_ID,
	ETL_D,
	ORIG_ETL_D,
	EROR_C 
FROM {{ ref('XfmBusinessRules__OutAppProdPurpRejectsDS') }}