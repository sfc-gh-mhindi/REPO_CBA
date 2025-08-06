{{ config(materialized='incremental', alias='_cba__app_hlt_sit_dataset_tmp__cse__clp__bus__appt__rel__appt__rel__lp__20080310', incremental_strategy='insert_overwrite', tags=['XfmCPL_APP_PROD_Appt_RelExt']) }}

SELECT
	APPT_I,
	RELD_APPT_I,
	EFFT_D,
	REL_TYPE_C,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I,
	RUN_STRM 
FROM {{ ref('XfmBusinessRules__OutTmpApptTrnfDetlDS') }}