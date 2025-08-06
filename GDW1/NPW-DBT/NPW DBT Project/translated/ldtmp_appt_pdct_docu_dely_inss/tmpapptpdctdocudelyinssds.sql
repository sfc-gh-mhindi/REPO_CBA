{{ config(materialized='view', tags=['LdTMP_APPT_PDCT_DOCU_DELY_INSS']) }}

WITH 
_cba__app_hlt_dev_dataset_tmp__cse__chl__bus__tu__app__appt__pdct__docu__dely__inss AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_dev_dataset_tmp__cse__chl__bus__tu__app__appt__pdct__docu__dely__inss")  }})
TmpApptPdctDocuDelyInssDS AS (
	SELECT APPT_PDCT_I,
		EFFT_D,
		DOCU_DELY_METH_C,
		PYAD_TYPE_C,
		ADRS_LINE_1_X,
		ADRS_LINE_2_X,
		SURB_X,
		PCOD_C,
		STAT_X,
		ISO_CNTY_C,
		BRCH_N,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		RUN_STRM
	FROM _cba__app_hlt_dev_dataset_tmp__cse__chl__bus__tu__app__appt__pdct__docu__dely__inss
)

SELECT * FROM TmpApptPdctDocuDelyInssDS