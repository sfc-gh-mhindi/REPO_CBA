{{ config(materialized='view', tags=['LdWRK_APPT_PDCT_FEAT_HL_FEE_DISCFrmXfm']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_wrk__cse__chl__bus__fee__disc__fee__appt__pdct__feat AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_wrk__cse__chl__bus__fee__disc__fee__appt__pdct__feat")  }})
SrcApptPdctFeatDS AS (
	SELECT APPT_PDCT_I,
		FEAT_I,
		SRCE_SYST_APPT_FEAT_I,
		EFFT_D,
		SRCE_SYST_C,
		SRCE_SYST_APPT_OVRD_I,
		OVRD_FEAT_I,
		SRCE_SYST_STND_VALU_Q,
		SRCE_SYST_STND_VALU_R,
		SRCE_SYST_STND_VALU_A,
		CNCY_C,
		ACTL_VALU_Q,
		ACTL_VALU_R,
		ACTL_VALU_A,
		FEAT_SEQN_N,
		FEAT_STRT_D,
		FEE_CHRG_D,
		OVRD_REAS_C,
		FEE_ADD_TO_TOTL_F,
		FEE_CAPL_F,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		HL_FEE_ID,
		BF_HL_FEE_ID,
		BF_HL_APP_PROD_ID,
		BF_XML_CODE,
		BF_DISPLAY_NAME,
		BF_CATEGORY,
		BF_UNIT_AMOUNT,
		BF_TOTAL_AMOUNT,
		BF_FOUND_FLAG,
		BFD_HL_FEE_DISCOUNT_ID,
		BFD_HL_FEE_ID,
		BFD_DISCOUNT_REASON,
		BFD_DISCOUNT_CODE,
		BFD_DISCOUNT_AMT,
		BFD_DISCOUNT_TERM,
		BFD_HL_FEE_DISCOUNT_CAT_ID,
		BFD_FOUND_FLAG,
		ORIG_ETL_D
	FROM _cba__app_csel4_csel4dev_dataset_wrk__cse__chl__bus__fee__disc__fee__appt__pdct__feat
)

SELECT * FROM SrcApptPdctFeatDS