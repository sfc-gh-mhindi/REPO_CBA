{{ config(materialized='view', tags=['XfmEnvtUser']) }}

WITH XfmInserts AS (
	SELECT
		EVNT_I,
		USER_I,
		EVNT_PATY_ROLE_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I
	FROM {{ ref('LeftJoinEvntUser') }}
	WHERE {{ ref('LeftJoinEvntUser') }}.dummy = 0
)

SELECT * FROM XfmInserts