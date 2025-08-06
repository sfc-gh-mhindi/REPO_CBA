{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_hl__pl__cc__app__prod__acct__appt__pdct', incremental_strategy='insert_overwrite', tags=['XfmDelFlagACCT_APPT_PDCT']) }}

SELECT
	ACCT_I,
	APPT_PDCT_I,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmTransform__OutAcctApptPdctDS2') }}