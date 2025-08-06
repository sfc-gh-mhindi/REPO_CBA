{{ config(materialized='incremental', alias='aset_adrs', incremental_strategy='append', tags=['LdAsetAdrsIns']) }}

SELECT
	ASET_I
	ADRS_I
	SRCE_SYST_C
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtAsetAdrsInsertDS') }}