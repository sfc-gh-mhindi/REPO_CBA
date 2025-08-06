{{ config(materialized='view', tags=['LdAPPT_PDCT_DOCU_DELY_INSSIns']) }}

WITH 
_cba__app_hlt_dev_dataset_appt__pdct__docu__dely__inss__i__cse__chl__bus__tu__app__20071010 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_dev_dataset_appt__pdct__docu__dely__inss__i__cse__chl__bus__tu__app__20071010")  }})
TgtAPPT_PDCT_DOCU_DELY_INSSInsertDS AS (
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
		EROR_SEQN_I
	FROM _cba__app_hlt_dev_dataset_appt__pdct__docu__dely__inss__i__cse__chl__bus__tu__app__20071010
)

SELECT * FROM TgtAPPT_PDCT_DOCU_DELY_INSSInsertDS