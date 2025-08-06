{{ config(materialized='incremental', alias='appt_aset_setl_locn', incremental_strategy='append', tags=['LdApptAsetSetlLocnFrmCseChlBusHlmAppSecIns']) }}

SELECT
	APPT_I
	ASET_I
	SRCE_SYST_C
	FRWD_DOCU_C
	SETL_LOCN_X
	SETL_CMMT_X
	efft_d
	expy_d
	pros_key_efft_i
	pros_key_expy_i 
FROM {{ ref('TgtApptAsetSetlLocnInsertDS') }}