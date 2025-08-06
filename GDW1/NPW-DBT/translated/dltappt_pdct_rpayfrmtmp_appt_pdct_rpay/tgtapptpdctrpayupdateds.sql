{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_appt__pdct__feat__u__cse__cpl__bus__fee__margin__20060101', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_RPAYFrmTMP_APPT_PDCT_RPAY']) }}

SELECT
	APPT_PDCT_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptPdctRpayUpdateDS') }}