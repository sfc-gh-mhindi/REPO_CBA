{{ config(materialized='view', tags=['LdApptStusInsert']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__stus__i__cse__com__cpo__bus__ncpr__clnt__20101011 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__stus__i__cse__com__cpo__bus__ncpr__clnt__20101011")  }})
ApptPdctInsert AS (
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
		EROR_SEQN_I
	FROM _cba__app_csel4_dev_dataset_appt__stus__i__cse__com__cpo__bus__ncpr__clnt__20101011
)

SELECT * FROM ApptPdctInsert