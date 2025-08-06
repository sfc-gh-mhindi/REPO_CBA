{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cse__cpl__bus__app__plapp__nulls__rejects', incremental_strategy='insert_overwrite', tags=['ExtPL_APP']) }}

SELECT
	PL_APP_ID,
	NOMINATED_BRANCH_ID,
	PL_PACKAGE_CAT_ID,
	ETL_D,
	ORIG_ETL_D,
	EROR_C 
FROM {{ ref('XfmCheckPlAppIdNulls__OutPlAppIdNullsDS') }}