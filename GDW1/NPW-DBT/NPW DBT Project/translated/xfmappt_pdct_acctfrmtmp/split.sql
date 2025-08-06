{{ config(materialized='view', tags=['XfmAppt_Pdct_AcctFrmTmp']) }}

WITH split AS (
	SELECT
		-- *SRC*: Field(Locn_Serv_Curr_And_Idnn_Error.RJCT_RECD, '|', 1),
		FIELD({{ ref('UTIL_ACCT_IDNN_EROR') }}.RJCT_RECD, '|', 1) AS APPT_PDCT_I,
		ACCT_I,
		-- *SRC*: Field(Locn_Serv_Curr_And_Idnn_Error.RJCT_RECD, '|', 3),
		FIELD({{ ref('UTIL_ACCT_IDNN_EROR') }}.RJCT_RECD, '|', 3) AS REL_TYPE_C,
		-- *SRC*: Field(Locn_Serv_Curr_And_Idnn_Error.RJCT_RECD, '|', 4),
		FIELD({{ ref('UTIL_ACCT_IDNN_EROR') }}.RJCT_RECD, '|', 4) AS EFFT_D,
		-- *SRC*: Field(Locn_Serv_Curr_And_Idnn_Error.RJCT_RECD, '|', 6),
		FIELD({{ ref('UTIL_ACCT_IDNN_EROR') }}.RJCT_RECD, '|', 6) AS PROS_KEY_EFFT_I,
		'1' AS ERR_FLG,
		-- *SRC*: Field(Locn_Serv_Curr_And_Idnn_Error.RJCT_RECD, '|', 5),
		FIELD({{ ref('UTIL_ACCT_IDNN_EROR') }}.RJCT_RECD, '|', 5) AS EXPY_D,
		-- *SRC*: \(20)If (Field(Locn_Serv_Curr_And_Idnn_Error.RJCT_RECD, '|', 7)) = '<null>' Then SetNull() Else Field(Locn_Serv_Curr_And_Idnn_Error.RJCT_RECD, '|', 7),
		IFF(FIELD({{ ref('UTIL_ACCT_IDNN_EROR') }}.RJCT_RECD, '|', 7) = '<null>', SETNULL(), FIELD({{ ref('UTIL_ACCT_IDNN_EROR') }}.RJCT_RECD, '|', 7)) AS PROS_KEY_EXPY_I,
		-- *SRC*: \(20)If (Field(Locn_Serv_Curr_And_Idnn_Error.RJCT_RECD, '|', 8)) = '<null>' Then SetNull() Else Field(Locn_Serv_Curr_And_Idnn_Error.RJCT_RECD, '|', 8),
		IFF(FIELD({{ ref('UTIL_ACCT_IDNN_EROR') }}.RJCT_RECD, '|', 8) = '<null>', SETNULL(), FIELD({{ ref('UTIL_ACCT_IDNN_EROR') }}.RJCT_RECD, '|', 8)) AS EROR_SEQN_I,
		ACCOUNT_NUMBER,
		REPAYMENT_ACCOUNT_NUMBER
	FROM {{ ref('UTIL_ACCT_IDNN_EROR') }}
	WHERE {{ ref('UTIL_ACCT_IDNN_EROR') }}.ACCT_I IS NOT NULL
)

SELECT * FROM split