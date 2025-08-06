{{ config(materialized='incremental', alias='appt_pdct_unid_paty', incremental_strategy='append', tags=['LdApptGnrcDate_Ins']) }}

SELECT
	APPT_I
	DATE_ROLE_C
	EFFT_D
	GNRC_ROLE_S
	GNRC_ROLE_D
	GNRC_ROLE_T
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I
	MODF_S
	MODF_D
	MODF_T
	USER_I
	CHNG_REAS_TYPE_C 
FROM {{ ref('TgtApptgnrcDtInsertDS') }}