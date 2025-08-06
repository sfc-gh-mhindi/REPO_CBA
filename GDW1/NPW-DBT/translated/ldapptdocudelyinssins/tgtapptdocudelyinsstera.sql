{{ config(materialized='incremental', alias='appt_docu_dely_inss', incremental_strategy='append', tags=['LdApptDocuDelyInssIns']) }}

SELECT
	APPT_I
	SRCE_SYST_C
	DOCU_DELY_RECV_C
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I 
FROM {{ ref('Cpy') }}