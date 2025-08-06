{{ config(materialized='view', tags=['LdTMP_APPT_REL_LPFrmXfm']) }}

WITH 
_cba__app_hlt_sit_dataset_tmp__cse__clp__bus__appt__rel__appt__rel__lp__20080310 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_hlt_sit_dataset_tmp__cse__clp__bus__appt__rel__appt__rel__lp__20080310")  }})
SrcApptRelDS AS (
	SELECT APPT_I,
		RELD_APPT_I,
		REL_TYPE_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		RUN_STRM
	FROM _cba__app_hlt_sit_dataset_tmp__cse__clp__bus__appt__rel__appt__rel__lp__20080310
)

SELECT * FROM SrcApptRelDS