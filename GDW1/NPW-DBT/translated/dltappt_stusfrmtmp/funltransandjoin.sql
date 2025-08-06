{{ config(materialized='view', tags=['DltAppt_StusFrmTMP']) }}

WITH FunlTransAndJoin AS (
	SELECT
		APPT_I as APPT_I,
		STUS_C as STUS_C,
		STRT_S as STRT_S,
		STRT_D as STRT_D,
		STRT_T as STRT_T,
		END_D as END_D,
		END_T as END_T,
		END_S as END_S,
		EMPL_I as EMPL_I,
		EFFT_D as EFFT_D,
		EXPY_D as EXPY_D,
		PROS_KEY_EFFT_I as PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I as PROS_KEY_EXPY_I,
		EROR_SEQN_I as EROR_SEQN_I,
		change_code as change_code
	FROM {{ ref('Join') }}
	UNION ALL
	SELECT
		APPT_I,
		STUS_C,
		STRT_D,
		STRT_T,
		STRT_S,
		END_D,
		END_T,
		END_S,
		EMPL_I,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		change_code
	FROM {{ ref('Transfrm__ToColGen') }}
)

SELECT * FROM FunlTransAndJoin