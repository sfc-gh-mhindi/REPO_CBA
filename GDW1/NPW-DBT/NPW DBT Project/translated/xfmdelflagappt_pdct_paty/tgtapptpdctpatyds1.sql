{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_app__prod__client__role__appt__pdct__paty', incremental_strategy='insert_overwrite', tags=['XfmDelFlagAPPT_PDCT_PATY']) }}

SELECT
	APPT_PDCT_I,
	PATY_I,
	PATY_ROLE_C,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmTransform__OutApptPdctPatyDS1') }}