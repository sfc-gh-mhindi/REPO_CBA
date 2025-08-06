{{ config(materialized='incremental', alias='util_acct_idnn_eror', incremental_strategy='append', tags=['LdUtilAcctIdnnErorInsCommon']) }}

SELECT
	EROR_ACCT_NUMB
	CONV_M
	EROR_TABL_NAME
	EROR_COLM_NAME
	RJCT_REAS
	LOAD_S
	PROS_KEY_EFFT_I
	EXPY_DATE
	PROS_KEY_EXPY_I
	ROW_SECU_ACCS_C
	RJCT_RECD 
FROM {{ ref('UTIL_ACCT_IDNN_EROR_I') }}