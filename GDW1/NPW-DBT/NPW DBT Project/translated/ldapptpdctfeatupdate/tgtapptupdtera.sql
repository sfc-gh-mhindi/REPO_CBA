{{ config(materialized='incremental', alias='appt', incremental_strategy='merge', tags=['LdApptPdctFeatUpdate']) }}

SELECT
	APPT_PDCT_I
	FEAT_I
	SRCE_SYST_C
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptInsertDS') }}