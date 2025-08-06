{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_appt__pdct__feat__i__cse__chl__bus__app__20150115', incremental_strategy='insert_overwrite', tags=['DltCSE_APPT_HL_COND_APPR_FrmTMPAPPTHLCONDAPPR_HSCA']) }}

SELECT
	APPT_I,
	EFFT_D,
	COND_APPR_F,
	COND_APPR_CONV_TO_FULL_D,
	EXPY_D,
	ROW_SECU_ACCS_C,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptHlCondApprInsertDS') }}