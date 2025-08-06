{{ config(materialized='view', tags=['XfmApptPdctAcctToTmp']) }}

WITH AccNum__AccntNmbr AS (
	SELECT
		-- *SRC*: \(20)If IsNull(AcctNumb.APP_PROD_ID) Then 'Y' Else  If Trim(AcctNumb.APP_PROD_ID) = '' Then 'Y' Else  if Trim(AcctNumb.APP_PROD_ID) = '0' Then 'Y' Else 'N',
		IFF({{ ref('Copy') }}.APP_PROD_ID IS NULL, 'Y', IFF(TRIM({{ ref('Copy') }}.APP_PROD_ID) = '', 'Y', IFF(TRIM({{ ref('Copy') }}.APP_PROD_ID) = '0', 'Y', 'N'))) AS svIsNullAppProdId,
		-- *SRC*: \(20)If IsNull(AcctNumb.REPAYMENT_ACCOUNT_NUMBER) Then 'Y' Else  if Trim(AcctNumb.REPAYMENT_ACCOUNT_NUMBER) = '0' Then 'Y' Else  if Trim(( IF IsNotNull((AcctNumb.REPAYMENT_ACCOUNT_NUMBER)) THEN (AcctNumb.REPAYMENT_ACCOUNT_NUMBER) ELSE "")) = '' THEN 'Y' ELSE 'N',
		IFF({{ ref('Copy') }}.REPAYMENT_ACCOUNT_NUMBER IS NULL, 'Y', IFF(TRIM({{ ref('Copy') }}.REPAYMENT_ACCOUNT_NUMBER) = '0', 'Y', IFF(TRIM(IFF({{ ref('Copy') }}.REPAYMENT_ACCOUNT_NUMBER IS NOT NULL, {{ ref('Copy') }}.REPAYMENT_ACCOUNT_NUMBER, '')) = '', 'Y', 'N'))) AS svIsNullRepayAccountNumber,
		-- *SRC*: \(20)If IsNull(AcctNumb.ACCOUNT_NUMBER) Then 'Y' Else  if Trim(AcctNumb.ACCOUNT_NUMBER) = '0' Then 'Y' Else  if Trim(( IF IsNotNull((AcctNumb.ACCOUNT_NUMBER)) THEN (AcctNumb.ACCOUNT_NUMBER) ELSE "")) = '' THEN 'Y' ELSE 'N',
		IFF({{ ref('Copy') }}.ACCOUNT_NUMBER IS NULL, 'Y', IFF(TRIM({{ ref('Copy') }}.ACCOUNT_NUMBER) = '0', 'Y', IFF(TRIM(IFF({{ ref('Copy') }}.ACCOUNT_NUMBER IS NOT NULL, {{ ref('Copy') }}.ACCOUNT_NUMBER, '')) = '', 'Y', 'N'))) AS svIsNullAccountNumber,
		-- *SRC*: 'CSEPO' : AcctNumb.APP_PROD_ID,
		CONCAT('CSEPO', {{ ref('Copy') }}.APP_PROD_ID) AS APPT_PDCT_I,
		ACCOUNT_NUMBER,
		REPAYMENT_ACCOUNT_NUMBER,
		'RPAY' AS REL_TYPE_C,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS EFFT_D,
		'9999-12-31' AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		'0' AS ERR_FLG
	FROM {{ ref('Copy') }}
	WHERE svIsNullAppProdId = 'N' AND svIsNullRepayAccountNumber = 'N'
)

SELECT * FROM AccNum__AccntNmbr