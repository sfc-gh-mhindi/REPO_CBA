{{ config(materialized='view', tags=['DltAppt_StusFrmTMP']) }}

WITH Transfrm__Appt_Pdct AS (
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
		EROR_SEQN_I
	FROM {{ ref('Data_Set_274') }}
	WHERE {{ ref('Data_Set_274') }}.keyChange = 1
)

SELECT * FROM Transfrm__Appt_Pdct