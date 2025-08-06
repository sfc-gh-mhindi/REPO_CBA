{{ config(materialized='incremental', alias='antn', incremental_strategy='append', tags=['LdAntnIns']) }}

SELECT
	{{ ref('ANTN_I') }}
	ANTN_TYPE_C
	ANTN_X
	SRCE_SYST_C
	SRCE_SYST_ANTN_I
	ANTN_S
	ANTN_D
	ANTN_T
	EMPL_I
	USER_I
	PRVT_F
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('ANTN_I') }}