{{ config(materialized='view', tags=['DltAppt_StusFrmTMP']) }}

WITH gdw_cpy AS (
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
		EFFT_D
	FROM {{ ref('Appt_Stus_Tgt') }}
)

SELECT * FROM gdw_cpy