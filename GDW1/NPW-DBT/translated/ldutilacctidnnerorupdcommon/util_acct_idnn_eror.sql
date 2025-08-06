{{ config(materialized='incremental', alias='util_acct_idnn_eror', incremental_strategy='merge', tags=['LdUtilAcctIdnnErorUpdCommon']) }}

SELECT
	EROR_ACCT_NUMB
	PROS_KEY_EFFT_I
	EXPY_DATE
	PROS_KEY_EXPY_I 
FROM {{ ref('Rem_Dup') }}