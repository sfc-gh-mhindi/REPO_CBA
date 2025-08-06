{{ config(materialized='incremental', alias='appt_pdct_acct', incremental_strategy='append', tags=['LdApptStusIns']) }}

SELECT
	APPT_I
	STUS_C
	STRT_S
	STRT_D
	STRT_T
	END_D
	END_T
	END_S
	EMPL_I
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtApptStusInsertDS') }}