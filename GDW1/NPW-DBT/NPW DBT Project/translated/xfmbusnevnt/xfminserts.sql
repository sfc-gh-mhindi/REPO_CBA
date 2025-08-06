{{ config(materialized='view', tags=['XfmBusnEvnt']) }}

WITH XfmInserts AS (
	SELECT
		EVNT_I,
		SRCE_SYST_EVNT_I,
		EVNT_ACTL_D,
		SRCE_SYST_C,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		EROR_SEQN_I,
		SRCE_SYST_EVNT_TYPE_I,
		EVNT_ACTL_T,
		ROW_SECU_ACCS_C
	FROM {{ ref('LeftJoinBusnEvnt') }}
	WHERE {{ ref('LeftJoinBusnEvnt') }}.dummy = 0
)

SELECT * FROM XfmInserts