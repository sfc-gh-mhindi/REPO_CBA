{{ config(materialized='incremental', alias='_cba__app_csel4_csel4__prd_dataset_cse__clp__bus__appt__pdct__feat__merge__20080405', incremental_strategy='insert_overwrite', tags=['MergeAppProdClpAppProd']) }}

SELECT
	APP_PROD_ID,
	COM_SUBTYPE_CODE,
	CAMPAIGN_CAT_ID,
	COM_APP_ID 
FROM {{ ref('Identify_IDs_ComPlAppProd') }}