{{ config(materialized='view', tags=['LdTMP_APPT_STUS_REASFrmXfm']) }}

WITH 
_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__stus__reas AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__stus__reas")  }})
SrcApptStusReasDS AS (
	SELECT APPT_I,
		STUS_C,
		STUS_REAS_TYPE_C,
		STRT_S,
		END_S,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		RUN_STRM
	FROM _cba__app_csel4_csel4dev_dataset_tmp__cse__com__bus__ccl__chl__com__app__appt__stus__reas
)

SELECT * FROM SrcApptStusReasDS