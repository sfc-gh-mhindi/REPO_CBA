{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cc__app__prod__bal__xfer__appt__trnf__detl', incremental_strategy='insert_overwrite', tags=['XfmDelFlagAPPT_TRNF_DETL']) }}

SELECT
	APPT_I,
	APPT_TRNF_I,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmTransform') }}