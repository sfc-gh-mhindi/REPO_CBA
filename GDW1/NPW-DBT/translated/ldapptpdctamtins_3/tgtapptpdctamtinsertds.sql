{{ config(materialized='view', tags=['LdApptPdctAmtIns_3']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__pdct__amt__3__i__cse__chl__bus__app__prod__purp__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__pdct__amt__3__i__cse__chl__bus__app__prod__purp__20060101")  }})
TgtApptPdctAmtInsertDS AS (
	SELECT APPT_PDCT_I,
		AMT_TYPE_C,
		EFFT_D,
		EXPY_D,
		CNCY_C,
		APPT_PDCT_A,
		SRCE_SYST_C,
		XCES_AMT_REAS_X,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		CNCY_CONV_R,
		DISC_CNCY_CONV_R,
		DISC_CNCY_DEAL_AUTN_N,
		SRCE_SYST_APPT_PDCT_AMT_I,
		APPT_PDCT_AUD_EQAL_A,
		PAYT_METH_TYPE_C
	FROM _cba__app_csel4_csel4dev_dataset_appt__pdct__amt__3__i__cse__chl__bus__app__prod__purp__20060101
)

SELECT * FROM TgtApptPdctAmtInsertDS