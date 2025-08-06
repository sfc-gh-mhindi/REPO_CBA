{{ config(materialized='incremental', alias='tmp_appt_gnrc_date', incremental_strategy='append', tags=['LdTMP_APPT_GNRC_DATEFrmXfm']) }}

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
FROM {{ ref('TmpCseAppt_Gnrc_Date') }}