{{ config(materialized='view', tags=['XfmAppt_Stus']) }}

WITH SrtApptI AS (
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
		-- *SRC*: KeyChange(),
		KEYCHANGE() AS keyChange
	FROM {{ ref('SrtDesc') }}
	ORDER BY APPT_I DESC
)

SELECT * FROM SrtApptI