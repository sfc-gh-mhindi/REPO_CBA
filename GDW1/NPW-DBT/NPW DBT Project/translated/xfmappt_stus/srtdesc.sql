{{ config(materialized='view', tags=['XfmAppt_Stus']) }}

WITH SrtDesc AS (
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
		RUN_STRM,
		SM_STATE_CAT_ID
	FROM {{ ref('Xfm__To_TgtApptPdct') }}
	ORDER BY APPT_I DESC, STRT_S DESC, END_S DESC
)

SELECT * FROM SrtDesc