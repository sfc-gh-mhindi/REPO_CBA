{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_cse__com__bus__ccl__chl__com__app__map__cse__appt__orig__20130605', incremental_strategy='insert_overwrite', tags=['ExtMapCseApptOrig']) }}

SELECT
	CHNL_CAT_ID,
	APPT_ORIG_C,
	EFFT_D,
	EXPY_D 
FROM {{ ref('tc_MapCseApptorig') }}