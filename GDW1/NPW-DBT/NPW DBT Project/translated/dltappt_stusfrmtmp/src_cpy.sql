{{ config(materialized='view', tags=['DltAppt_StusFrmTMP']) }}

WITH src_cpy AS (
	SELECT
		APPT_I,
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
	FROM {{ ref('Transfrm__Appt_Pdct') }}
)

SELECT * FROM src_cpy