{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_pl__appt__pdct__unid__paty', incremental_strategy='insert_overwrite', tags=['XfmDelFlagAPPT_PDCT_UNID_PATY']) }}

SELECT
	APPT_PDCT_I,
	PATY_ROLE_C,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmTransform') }}