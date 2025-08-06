{{ config(materialized='view', tags=['LdTmp_Appt_StusFrmXfmr']) }}

WITH 
_cba__app_csel4_sit_dataset_tmp__cse__com__cpo__bus__ncpr__clnt__appt__stus AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_sit_dataset_tmp__cse__com__cpo__bus__ncpr__clnt__appt__stus")  }})
TgtAppt_Stus AS (
	SELECT APPT_I,
		STUS_C,
		STRT_S,
		STRT_D,
		STRT_T,
		END_D,
		END_T,
		END_S,
		EMPL_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		RUN_STRM
	FROM _cba__app_csel4_sit_dataset_tmp__cse__com__cpo__bus__ncpr__clnt__appt__stus
)

SELECT * FROM TgtAppt_Stus