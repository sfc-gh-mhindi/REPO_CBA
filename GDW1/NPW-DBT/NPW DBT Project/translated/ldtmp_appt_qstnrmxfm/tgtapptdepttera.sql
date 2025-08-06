{{ config(materialized='incremental', alias='tmp_appt_qstn', incremental_strategy='append', tags=['LdTMP_APPT_QSTNrmXfm']) }}

SELECT
	APPT_I
	QSTN_C
	EFFT_D
	RESP_C
	RESP_CMMT_X
	EXPY_D
	PATY_I
	ROW_SECU_ACCS_C
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	EROR_SEQN_I
	RUN_STRM 
FROM {{ ref('SrcApptDeptDS') }}