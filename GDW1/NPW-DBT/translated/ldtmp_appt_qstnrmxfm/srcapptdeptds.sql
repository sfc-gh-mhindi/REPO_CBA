{{ config(materialized='view', tags=['LdTMP_APPT_QSTNrmXfm']) }}

WITH 
_cba__app_hlt_sit_dataset_tmp__cse__clp__bus__appt__qstn__appt__qstn__detl AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_sit_dataset_tmp__cse__clp__bus__appt__qstn__appt__qstn__detl")  }})
SrcApptDeptDS AS (
	SELECT APPT_I,
		QSTN_C,
		EFFT_D,
		RESP_C,
		RESP_CMMT_X,
		EXPY_D,
		PATY_I,
		ROW_SECU_ACCS_C,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		RUN_STRM
	FROM _cba__app_hlt_sit_dataset_tmp__cse__clp__bus__appt__qstn__appt__qstn__detl
)

SELECT * FROM SrcApptDeptDS