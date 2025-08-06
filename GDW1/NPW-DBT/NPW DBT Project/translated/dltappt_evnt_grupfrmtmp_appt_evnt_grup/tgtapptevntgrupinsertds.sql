{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__pdct__feat__i__cse__cpl__bus__fee__margin__20170307', incremental_strategy='insert_overwrite', tags=['DltAPPT_EVNT_GRUPFrmTMP_APPT_EVNT_GRUP']) }}

SELECT
	APPT_I,
	EVNT_GRUP_I,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptEvntGrupInsertDS') }}