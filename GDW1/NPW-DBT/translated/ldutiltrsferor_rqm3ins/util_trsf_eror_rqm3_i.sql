{{ config(materialized='view', tags=['LdUtilTrsfEror_Rqm3Ins']) }}

WITH 
_cba__app_csel4_dev_dataset_util__trsf__eror__rqm3__i__cse__chl__bus__hlm__app__20100614 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_util__trsf__eror__rqm3__i__cse__chl__bus__hlm__app__20100614")  }})
UTIL_TRSF_EROR_RQM3_I AS (
	SELECT SRCE_KEY_I,
		CONV_M,
		CONV_MAP_RULE_M,
		TRSF_TABL_M,
		SRCE_EFFT_D,
		VALU_CHNG_BFOR_X,
		VALU_CHNG_AFTR_X,
		TRSF_X,
		TRSF_COLM_M,
		EROR_SEQN_I,
		SRCE_FILE_M,
		PROS_KEY_EFFT_I,
		TRSF_KEY_I
	FROM _cba__app_csel4_dev_dataset_util__trsf__eror__rqm3__i__cse__chl__bus__hlm__app__20100614
)

SELECT * FROM UTIL_TRSF_EROR_RQM3_I