{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__ccc__bus__app__prod__bal__xfer__premap', incremental_strategy='insert_overwrite', tags=['ExtCC_APP_PROD_BAL_XFER']) }}

SELECT
	CC_APP_PROD_BAL_XFER_ID,
	BAL_XFER_OPTION_CAT_ID,
	XFER_AMT,
	BAL_XFER_INSTITUTION_CAT_ID,
	CC_APP_PROD_ID,
	CC_APP_ID,
	ORIG_ETL_D 
FROM {{ ref('FunMergeJournal') }}