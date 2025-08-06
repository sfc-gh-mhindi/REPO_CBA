{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_paty__appt__pdct__u__cse__com__bus__app__prod__clnt__rl__20150728', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_PATYFrmTMP_APPT_PDCT_PATY_2']) }}

SELECT
	APPT_PDCT_I,
	PATY_I,
	PATY_ROLE_C,
	EFFT_D,
	PROS_KEY_EXPY_I,
	EXPY_D 
FROM {{ ref('XfmCheckDeltaAction__OutTgtPatyApptPdctUpdateDS') }}