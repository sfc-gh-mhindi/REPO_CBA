{{ config(materialized='incremental', alias='appt_pdct_acct', incremental_strategy='append', tags=['LdApptAsesDetlIns']) }}

SELECT
	APPT_I
	AMT_TYPE_C
	EFFT_D
	EXPY_D
	CNCY_C
	APPT_ASES_A
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtApptInsertDS') }}