{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_cse__com__bus__ccl__chl__com__app__map__cse__appt__qlfy__20130605', incremental_strategy='insert_overwrite', tags=['ExtMapCseApptQlfy']) }}

SELECT
	SBTY_CODE,
	APPT_QLFY_C,
	EFFT_D,
	EXPY_D 
FROM {{ ref('tc_MapCseApptQlfy') }}