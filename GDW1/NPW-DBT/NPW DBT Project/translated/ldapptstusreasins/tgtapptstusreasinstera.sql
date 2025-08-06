{{ config(materialized='incremental', alias='appt_pdct_acct', incremental_strategy='append', tags=['LdApptStusReasIns']) }}

SELECT
	APPT_I
	STUS_C
	STUS_REAS_TYPE_C
	STRT_S
	END_S
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtApptStusReasInsertDS') }}