{{ config(materialized='incremental', alias='_cba__app_csel4_csel4__prd_dataset_cse__clp__bus__appt__pdct__feat__premap', incremental_strategy='insert_overwrite', tags=['ExtAppProdComAppProdClp']) }}

SELECT
	APP_PROD_ID,
	COM_SUBTYPE_CODE,
	CAMPAIGN_CAT_ID,
	COM_APP_ID,
	ORIG_ETL_D 
FROM {{ ref('FunMergeSourceAndRejt') }}