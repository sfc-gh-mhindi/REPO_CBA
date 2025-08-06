{{ config(materialized='view', tags=['LdUTIL_TRSF_EROR_R2Ins']) }}

WITH 
_cba__app_csel3_csel3dev_error___20060615__ AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel3_csel3dev_error___20060615__")  }})
ERROR_MERGE_TXT AS (
	SELECT SRCE_KEY_I,
		CONV_M,
		CONV_MAP_RULE_M,
		TRSF_TABL_M,
		VALU_CHNG_BFOR_X,
		VALU_CHNG_AFTR_X,
		TRSF_X,
		TRSF_COLM_M
	FROM _cba__app_csel3_csel3dev_error_*__20060615__*
)

SELECT * FROM ERROR_MERGE_TXT