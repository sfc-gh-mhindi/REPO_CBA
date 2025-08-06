{{ config(materialized='view', tags=['DltASETFrmTMP_ASET1']) }}

WITH XfmDltAset1__InsTgtAsetTera AS (
	SELECT
		{{ ref('JoinAll') }}.NEW_ASET_I AS ASET_I,
		{{ ref('JoinAll') }}.NEW_SECU_CODE_C AS SECU_CODE_C,
		SECU_CATG_C,
		SRCE_SYST_ASET_I,
		{{ ref('JoinAll') }}.NEW_SRCE_SYST_C AS SRCE_SYST_C,
		ASET_C,
		ORIG_SRCE_SYST_ASET_I,
		ORIG_SRCE_SYST_C,
		ENVT_F,
		ASET_X,
		EFFT_D,
		EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: setnull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		EROR_SEQN_I,
		ASET_LIBL_C,
		AL_CATG_C,
		DUPL_ASET_F
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = 1
)

SELECT * FROM XfmDltAset1__InsTgtAsetTera