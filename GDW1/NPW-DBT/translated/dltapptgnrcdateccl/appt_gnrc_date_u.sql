{{ config(materialized='incremental', alias='_cba__app01_csel4_dev_dataset_appt__gnrc__date__u__cse__ccl__cli__date__exp__aud__20100512', incremental_strategy='insert_overwrite', tags=['DltApptGnrcDateCCL']) }}

SELECT
	APPT_I,
	DATE_ROLE_C,
	EXPY_D,
	PROS_KEY_EXPY_I,
	EFFT_D 
FROM {{ ref('GetGDWFields') }}