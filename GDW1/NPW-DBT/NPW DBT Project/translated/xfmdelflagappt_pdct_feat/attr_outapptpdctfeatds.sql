{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_attr__appt__pdct__feat', incremental_strategy='insert_overwrite', tags=['XfmDelFlagAPPT_PDCT_FEAT']) }}

SELECT
	APPT_PDCT_I,
	SRCE_SYST_APPT_FEAT_I,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmTransform__ATTR_OutApptPdctFeat') }}