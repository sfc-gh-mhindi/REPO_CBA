{{ config(materialized='incremental', alias='appt_pdct_paty', incremental_strategy='append', tags=['LdApptQstnInsert']) }}

SELECT
	APPT_I
	QSTN_C
	RESP_C
	RESP_CMMT_X
	PATY_I
	EFFT_D
	EXPY_D
	ROW_SECU_ACCS_C
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('ApptQstnInsert') }}