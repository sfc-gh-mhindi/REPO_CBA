{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_cc__appt__pdct__feat', incremental_strategy='insert_overwrite', tags=['XfmDelFlagAPPT_PDCT_FEAT']) }}

SELECT
	APPT_PDCT_I,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmTransform__CC_OutApptPdctFeat') }}