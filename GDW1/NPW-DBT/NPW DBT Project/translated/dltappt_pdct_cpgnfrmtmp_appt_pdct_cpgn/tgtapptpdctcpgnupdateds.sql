{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__pdct__cpgn__u__cse__ccc__bus__app__prod__20060616', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_CPGNFrmTMP_APPT_PDCT_CPGN']) }}

SELECT
	APPT_PDCT_I,
	CPGN_TYPE_C,
	SRCE_SYST_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EXPY_I 
FROM {{ ref('xf_delta__ln_updates') }}