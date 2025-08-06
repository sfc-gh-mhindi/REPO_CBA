{{ config(materialized='view', tags=['LdApptFeatIns']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_appt__pdct__acct__i__cse__ccc__bus__app__prod__20060101 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_appt__pdct__acct__i__cse__ccc__bus__app__prod__20060101")  }})
TgtAppFeattInsertDS AS (
	SELECT APPT_I,
		FEAT_I,
		EFFT_D,
		SRCE_SYST_C,
		SRCE_SYST_APPT_FEAT_I,
		SRCE_SYST_STND_VALU_Q,
		SRCE_SYST_STND_VALU_R,
		SRCE_SYST_STND_VALU_A,
		ACTL_VALU_Q,
		ACTL_VALU_R,
		ACTL_VALU_A,
		CNCY_C,
		OVRD_FEAT_I,
		OVRD_REAS_C,
		FEAT_SEQN_N,
		FEAT_STRT_D,
		FEE_CHRG_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM _cba__app_csel4_csel4dev_dataset_appt__pdct__acct__i__cse__ccc__bus__app__prod__20060101
)

SELECT * FROM TgtAppFeattInsertDS