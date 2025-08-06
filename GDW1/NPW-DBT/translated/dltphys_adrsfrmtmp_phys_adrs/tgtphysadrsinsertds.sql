{{ config(materialized='incremental', alias='_cba__app_csel4_dev_dataset_phys__adrs__i__cse__chl__bus__prty__adrs__20110701', incremental_strategy='insert_overwrite', tags=['DltPHYS_ADRSFrmTMP_PHYS_ADRS']) }}

SELECT
	ADRS_I,
	PHYS_ADRS_TYPE_C,
	ADRS_LINE_1_X,
	ADRS_LINE_2_X,
	SURB_X,
	CITY_X,
	PCOD_C,
	STAT_C,
	ISO_CNTY_C,
	SRCE_SYST_C,
	EFFT_D,
	EXPY_D,
	PROS_KEY_EFFT_I,
	PROS_KEY_EXPY_I,
	ROW_SECU_ACCS_C 
FROM {{ ref('XfmCheckDeltaAction__OutTgtApptFeatInsertDS') }}