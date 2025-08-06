{{ config(materialized='view', tags=['LdAPP_ANS_EVNT_PATY_Ins']) }}

WITH XfmTouchUpdDummy__OutTgtUpdateDS AS (
	SELECT
		EVNT_I,
		SRCE_SYST_PATY_I,
		EVNT_PATY_ROLE_TYPE_C,
		EFFT_D,
		SRCE_SYST_C,
		PATY_I,
		EXPY_D,
		PROS_KEY_EFFT_I,
		PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		ROW_SECU_ACCS_C
	FROM {{ ref('TgtInsertDS') }}
	WHERE @FALSE
)

SELECT * FROM XfmTouchUpdDummy__OutTgtUpdateDS