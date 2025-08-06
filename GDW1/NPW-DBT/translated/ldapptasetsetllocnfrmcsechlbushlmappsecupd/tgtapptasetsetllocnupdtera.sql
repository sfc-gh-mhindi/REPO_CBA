{{ config(materialized='incremental', alias='appt_aset_setl_locn', incremental_strategy='merge', tags=['LdApptAsetSetlLocnFrmCseChlBusHlmAppSecUpd']) }}

SELECT
	APPT_I
	ASET_I
	EFFT_D
	SRCE_SYST_C
	EXPY_D
	pros_key_expy_i 
FROM {{ ref('TgtApptAsetSetlLocnUpdateDS') }}