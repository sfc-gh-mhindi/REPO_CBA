{{ config(materialized='incremental', alias='appt', incremental_strategy='merge', tags=['LdApptAsetUpd']) }}

SELECT
	APPT_I
	ASET_I
	EFFT_D
	EXPY_D
	pros_key_expy_i 
FROM {{ ref('TgtApptAsetUpdateDS') }}