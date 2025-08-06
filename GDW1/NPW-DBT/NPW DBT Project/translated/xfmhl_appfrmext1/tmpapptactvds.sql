{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__app__appt__actv', incremental_strategy='insert_overwrite', tags=['XfmHL_APPFrmExt1']) }}

SELECT
	APPT_I,
	APPT_ACTV_Q,
	RUN_STRM 
FROM {{ ref('RdHllApptId') }}