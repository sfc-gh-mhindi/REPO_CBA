{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_paty__appt__pdct__i__cse__com__bus__app__prod__clnt__rl__20150728', incremental_strategy='insert_overwrite', tags=['DltAPPT_PDCT_PATYFrmTMP_APPT_PDCT_PATY_2']) }}

SELECT
	APPT_PDCT_I,
	PATY_I,
	PATY_ROLE_C,
	EFFT_D,
	SRCE_SYST_C,
	SRCE_SYST_APPT_PDCT_PATY_I,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtPatyApptPdctInsertDS') }}