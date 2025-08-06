{{ config(materialized='view', tags=['LdUTIL_TRSF_EROR_R2_OD_Ins']) }}

WITH 
_cba__app_csel4_dev_error___r2__cse__com__cpo__bus__ncpr__clnt__20110110 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_error___r2__cse__com__cpo__bus__ncpr__clnt__20110110")  }})
ERROR_MERGE_TXT1 AS (
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
	FROM _cba__app_csel4_dev_error_*__r2__cse__com__cpo__bus__ncpr__clnt__20110110
)

SELECT * FROM ERROR_MERGE_TXT1