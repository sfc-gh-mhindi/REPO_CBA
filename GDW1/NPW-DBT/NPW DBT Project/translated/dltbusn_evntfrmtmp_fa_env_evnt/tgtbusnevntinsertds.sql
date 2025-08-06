{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_busn__evnt__i__cse__coi__bus__envi__evnt__20150610', incremental_strategy='insert_overwrite', tags=['DltBUSN_EVNTFrmTMP_FA_ENV_EVNT']) }}

SELECT
	EVNT_I,
	ROW_SECU_ACCS_C,
	SRCE_SYST_EVNT_I,
	SRCE_SYST_EVNT_TYPE_I,
	EVNT_ACTL_D,
	EVNT_ACTL_T,
	SRCE_SYST_C,
	PROS_KEY_EFFT_I,
	EROR_SEQN_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtBusnEvntInsertDS') }}