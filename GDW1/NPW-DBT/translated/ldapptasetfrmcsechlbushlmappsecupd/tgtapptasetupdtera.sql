{{ config(materialized='incremental', alias='appt_aset', incremental_strategy='merge', tags=['LdApptAsetFrmCseChlBusHlmAppSecUpd']) }}

SELECT
	APPT_I
	ASET_I
	EFFT_D
	EXPY_D
	pros_key_expy_i 
FROM {{ ref('TgtApptAsetUpdateDS') }}