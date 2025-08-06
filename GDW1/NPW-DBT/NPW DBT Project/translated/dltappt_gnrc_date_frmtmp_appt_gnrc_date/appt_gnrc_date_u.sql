{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__gnrc__date__u__cse__chl__bus__hlm__app__20100728', incremental_strategy='insert_overwrite', tags=['DltAPPT_GNRC_DATE_FrmTMP_APPT_GNRC_DATE']) }}

SELECT
	APPT_I,
	EXPY_D,
	PROS_KEY_EXPY_I,
	EFFT_D 
FROM {{ ref('Merge') }}