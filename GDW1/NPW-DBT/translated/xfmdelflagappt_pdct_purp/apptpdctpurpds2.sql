{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_hl__pl__app__prod__purp__appt__pdct__purp', incremental_strategy='insert_overwrite', tags=['XfmDelFlagAPPT_PDCT_PURP']) }}

SELECT
	APPT_PDCT_I,
	SRCE_SYST_APPT_PDCT_PURP_I,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmTransform__OutApptPdctPurpDS2') }}