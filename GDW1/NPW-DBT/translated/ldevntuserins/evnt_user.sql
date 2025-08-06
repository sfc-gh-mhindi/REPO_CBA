{{ config(materialized='incremental', alias='evnt_user', incremental_strategy='append', tags=['LdEvntUserIns']) }}

SELECT
	EVNT_I
	USER_I
	EVNT_PATY_ROLE_C
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('EVNT_USER_I') }}