{{ config(materialized='incremental', alias='appt_pdct_unid_paty', incremental_strategy='append', tags=['LdApptPdctUnidPatyIns']) }}

SELECT
	APPT_PDCT_I
	PATY_ROLE_C
	SRCE_SYST_PATY_I
	EFFT_D
	SRCE_SYST_C
	UNID_PATY_CATG_C
	PATY_M
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I 
FROM {{ ref('TgtApptInsertDS') }}