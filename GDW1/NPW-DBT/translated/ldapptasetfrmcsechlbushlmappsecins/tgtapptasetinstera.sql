{{ config(materialized='incremental', alias='appt_aset', incremental_strategy='append', tags=['LdApptAsetFrmCseChlBusHlmAppSecIns']) }}

SELECT
	APPT_I
	ASET_I
	PRIM_SECU_F
	efft_d
	expy_d
	pros_key_efft_i
	pros_key_expy_i
	eror_seqn_i
	ASET_SETL_REQD 
FROM {{ ref('TgtApptAsetInsertDS') }}