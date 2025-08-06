{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_ccl__app__fee__appt__feat', incremental_strategy='insert_overwrite', tags=['XfmDelFlagAPPT_FEAT']) }}

SELECT
	APPT_I,
	SRCE_SYST_APPT_FEAT_I,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmTransform') }}