{{ config(materialized='view', tags=['XfmEvntQstr']) }}

WITH XfmInserts AS (
	SELECT
		EVNT_I,
		QSTR_C,
		EFFT_D,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I
	FROM {{ ref('LeftOuterJoin') }}
	WHERE {{ ref('LeftOuterJoin') }}.dummy = 0
)

SELECT * FROM XfmInserts