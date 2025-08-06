{{ config(materialized='incremental', alias='appt', incremental_strategy='append', tags=['LdApptAsetIns']) }}

SELECT
	APPT_I
	ASET_I
	PRIM_SECU_F
	efft_d
	expy_d
	pros_key_efft_i
	pros_key_expy_i
	eror_seqn_i 
FROM {{ ref('TgtApptAsetInsertDS') }}