{{ config(materialized='incremental', alias='tmp_appt_qstn', incremental_strategy='append', tags=['LdTmp_Appt_QstnFrmXfm']) }}

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
FROM {{ ref('Remove_Duplicates') }}