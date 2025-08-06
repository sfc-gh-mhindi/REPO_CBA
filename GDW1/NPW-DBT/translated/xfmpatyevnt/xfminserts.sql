{{ config(materialized='view', tags=['XfmPatyEvnt']) }}

WITH XfmInserts AS (
	SELECT
		EVNT_I,
		SRCE_SYST_PATY_I,
		EVNT_PATY_ROLE_TYPE_C,
		SRCE_SYST_C,
		EFFT_D,
		PATY_I,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		ROW_SECU_ACCS_C
	FROM {{ ref('LeftJoinPatyEvnt') }}
	WHERE {{ ref('LeftJoinPatyEvnt') }}.dummy = 0
)

SELECT * FROM XfmInserts