{{ config(materialized='view', tags=['CCODSLdErr']) }}

WITH 
_cba__app_ccods_uat_inprocess_bccomp__util__trsf__eror__rqm3__co__mstr__20100827 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_ccods_uat_inprocess_bccomp__util__trsf__eror__rqm3__co__mstr__20100827")  }})
Err_File AS (
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
	FROM _cba__app_ccods_uat_inprocess_bccomp__util__trsf__eror__rqm3__co__mstr__20100827
)

SELECT * FROM Err_File