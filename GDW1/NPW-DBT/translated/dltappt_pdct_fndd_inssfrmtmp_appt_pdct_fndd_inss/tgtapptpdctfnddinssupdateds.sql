{{ config(materialized='incremental', alias='_cba__app_hlt_dev_dataset_appt__pdct__fndd__inss__u__cse__chl__bus__tu__app__fund__det__20071105', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_FNDD_INSSFrmTMP_APPT_PDCT_FNDD_INSS']) }}

SELECT
	APPT_PDCT_I,
	APPT_PDCT_FNDD_I,
	APPT_PDCT_FNDD_METH_I,
	FNDD_INSS_METH_C,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptPdctFnddInssUpdateDS') }}