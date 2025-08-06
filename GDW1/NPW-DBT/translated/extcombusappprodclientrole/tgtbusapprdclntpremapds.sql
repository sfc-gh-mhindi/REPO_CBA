{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__com__bus__app__prod__client__role__premap', incremental_strategy='insert_overwrite', tags=['ExtComBusAppProdClientRole']) }}

SELECT
	APP_PROD_CLIENT_ROLE_ID,
	ROLE_CAT_ID,
	CIF_CODE,
	APP_PROD_ID,
	SUBTYPE_CODE,
	ORIG_ETL_D 
FROM {{ ref('LkpExclusions') }}