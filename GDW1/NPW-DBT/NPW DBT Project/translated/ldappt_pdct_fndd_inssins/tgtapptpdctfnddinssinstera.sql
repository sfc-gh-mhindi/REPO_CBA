{{ config(materialized='incremental', alias='appt_pdct_fndd_inss', incremental_strategy='append', tags=['LdAPPT_PDCT_FNDD_INSSIns']) }}

SELECT
	APPT_PDCT_I
	APPT_PDCT_FNDD_I
	APPT_PDCT_FNDD_METH_I
	EFFT_D
	FNDD_INSS_METH_C
	SRCE_SYST_FNDD_I
	SRCE_SYST_FNDD_METH_I
	SRCE_SYST_C
	FNDD_D
	FNDD_A
	PDCT_SYST_ACCT_N
	CMPE_I
	CMPE_ACCT_BSB_N
	CMPE_ACCT_N
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtInsertDS') }}