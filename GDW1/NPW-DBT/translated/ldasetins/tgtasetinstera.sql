{{ config(materialized='incremental', alias='adrs', incremental_strategy='append', tags=['LdAsetIns']) }}

SELECT
	ASET_I
	SECU_CODE_C
	SECU_CATG_C
	SRCE_SYST_ASET_I
	SRCE_SYST_C
	ASET_C
	ORIG_SRCE_SYST_ASET_I
	ORIG_SRCE_SYST_C
	ENVT_F
	ASET_X
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I
	ASET_LIBL_C
	AL_CATG_C
	DUPL_ASET_F 
FROM {{ ref('TgtAsetInsertDS') }}