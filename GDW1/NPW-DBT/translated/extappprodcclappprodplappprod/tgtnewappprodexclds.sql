{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_cse__com__bus__app__prod__ccl__pl__app__prod__mapcseappprodexcl__insert', incremental_strategy='insert_overwrite', tags=['ExtAppProdCclAppProdPlAppProd']) }}

SELECT
	APP_PROD_ID,
	DUMMY_PDCT_F,
	CRAT_D 
FROM {{ ref('XfmSeparateRejectWithoutSourceAndTheRest__outTgtNewAppProdExclDS') }}