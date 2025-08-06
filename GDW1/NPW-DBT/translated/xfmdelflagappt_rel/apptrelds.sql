{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_ccl__hl__app__appt__rel', incremental_strategy='insert_overwrite', tags=['XfmDelFlagAPPT_REL']) }}

SELECT
	APPT_I,
	RELD_APPT_I,
	REL_TYPE_C,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmTransform') }}