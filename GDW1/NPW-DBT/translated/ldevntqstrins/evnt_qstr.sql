{{ config(materialized='incremental', alias='evnt_qstr', incremental_strategy='append', tags=['LdEvntQstrIns']) }}

SELECT
	EVNT_I
	QSTR_C
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I 
FROM {{ ref('EVNT_QSTR_I') }}