{{ config(materialized='view', tags=['LdTMP_ASETFrmXfm1']) }}

WITH 
_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlm__app__sec__aset AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlm__app__sec__aset")  }})
TgtTmp_AsetDS AS (
	SELECT ASET_I,
		SECU_CODE_C,
		SECU_CATG_C,
		SRCE_SYST_ASET_I,
		SRCE_SYST_C,
		ASET_C,
		ORIG_SRCE_SYST_ASET_I,
		ORIG_SRCE_SYST_C,
		ENVT_F,
		ASET_X,
		EFFT_D,
		EXPY_D,
		EROR_SEQN_I,
		ASET_LIBL_C,
		AL_CATG_C,
		DUPL_ASET_F,
		RUN_STRM
	FROM _cba__app_csel4_dev_dataset_tmp__cse__chl__bus__hlm__app__sec__aset
)

SELECT * FROM TgtTmp_AsetDS