{{ config(materialized='incremental', alias='_cba__app_hlt_dev_dataset_appt__pdct__docu__dely__inss__u__cse__chl__bus__tu__app__20071010', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_DOCU_DELY_INSSFrmTMP_APPT_PDCT_DOCU_DELY_INSS']) }}

SELECT
	APPT_PDCT_I,
	DOCU_DELY_METH_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtDeptApptUpdateDS') }}