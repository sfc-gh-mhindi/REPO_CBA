{{ config(materialized='incremental', alias='evnt_qstr_qstn_resp', incremental_strategy='append', tags=['LdEvntQstrQstnRespIns']) }}

SELECT
	EVNT_I
	QSTR_C
	QSTN_C
	RESP_C
	RESP_VALU_N
	RESP_VALU_S
	RESP_VALU_D
	RESP_VALU_T
	RESP_VALU_X
	EFFT_D
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	ROW_SECU_ACCS_C 
FROM {{ ref('EVNT_QSTR_QSTN_RESP_I') }}