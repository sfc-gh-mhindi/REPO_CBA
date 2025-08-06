{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_app__prod__client__role__paty__appt__pdct', incremental_strategy='insert_overwrite', tags=['XfmDelFlagPATY_APPT_PDCT']) }}

SELECT
	PATY_I,
	APPT_PDCT_I,
	PATY_ROLE_C,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmTransform__OutPatyApptPdctDS1') }}