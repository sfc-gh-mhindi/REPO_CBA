{{ config(materialized='incremental', alias='_cba__app_csel4_csel4dev_dataset_evnt__empl__i__cse__coi__bus__envi__evnt__20061016', incremental_strategy='insert_overwrite', tags=['DltEVNT_EMPLFrmTMP_FA_ENV_EVNT']) }}

SELECT
	EVNT_I,
	ROW_SECU_ACCS_C,
	EMPL_I,
	EVNT_PATY_ROLE_TYPE_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	EROR_SEQN_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtEvntEmplInsertDS') }}