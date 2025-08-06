{{ config(materialized='incremental', alias='_cba__app_csel4_csel4__prd_dataset_cse__clp__bus__appt__pdct__feat__mapping__rejects', incremental_strategy='insert_overwrite', tags=['XfmCPL_APP_PROD_Appt_Pdct_FeatExt']) }}

SELECT
	APP_PROD_ID,
	COM_SUBTYPE_CODE,
	CAMPAIGN_CAT_ID,
	COM_APP_ID,
	ETL_D,
	ORIG_ETL_D,
	EROR_C 
FROM {{ ref('XfmBusinessRules__REJ') }}