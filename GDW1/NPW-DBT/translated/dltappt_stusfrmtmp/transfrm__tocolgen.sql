{{ config(materialized='view', tags=['DltAppt_StusFrmTMP']) }}

WITH Transfrm__ToColGen AS (
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
		6 AS change_code
	FROM {{ ref('Data_Set_274') }}
	WHERE {{ ref('Data_Set_274') }}.keyChange = 0
)

SELECT * FROM Transfrm__ToColGen