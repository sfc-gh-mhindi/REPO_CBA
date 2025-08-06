{{ config(materialized='incremental', alias='tmp_appt_evnt_grup', incremental_strategy='append', tags=['LdTMP_APPT_EVNT_GRUPFrmXfm']) }}

SELECT
	APPT_I
	EVNT_GRUP_I
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I
	RUN_STRM 
FROM {{ ref('SrcApptEvntGrupDS') }}