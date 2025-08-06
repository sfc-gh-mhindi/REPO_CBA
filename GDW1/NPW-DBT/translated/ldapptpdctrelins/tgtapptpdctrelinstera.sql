{{ config(materialized='incremental', alias='appt_pdct_rel', incremental_strategy='append', tags=['LdApptPdctRelIns']) }}

SELECT
	APPT_PDCT_I
	RELD_APPT_PDCT_I
	EFFT_D
	REL_TYPE_C
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtApptPdctRelInsertDS') }}