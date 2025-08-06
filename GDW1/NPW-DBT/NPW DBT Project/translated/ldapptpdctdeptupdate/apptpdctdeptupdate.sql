{{ config(materialized='view', tags=['LdApptPdctDeptUpdate']) }}

WITH 
_cba__app_csel4_dev_dataset_appt__pdct__dept__u__cse__com__cpo__bus__ncpr__clnt__20110105 AS (
	SELECT
	*
	FROM {{ source("","_cba__app_csel4_dev_dataset_appt__pdct__dept__u__cse__com__cpo__bus__ncpr__clnt__20110105")  }})
ApptPdctDeptUpdate AS (
	SELECT APPT_PDCT_I,
		DEPT_ROLE_C,
		SRCE_SYST_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EXPY_I
	FROM _cba__app_csel4_dev_dataset_appt__pdct__dept__u__cse__com__cpo__bus__ncpr__clnt__20110105
)

SELECT * FROM ApptPdctDeptUpdate