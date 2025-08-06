{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_hl__pl__app__prod__appt__pdct__rpay', incremental_strategy='insert_overwrite', tags=['XfmDelFlagAPPT_PDCT_RPAY']) }}

SELECT
	APPT_PDCT_I,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmTransform') }}