{{ config(materialized='view', tags=['XfmAppt_Pdct_Amt']) }}

WITH Xfm__Laap AS (
	SELECT
		-- *SRC*: \(20)if IsNull(Src_to_Xfm.LIMIT_AMOUNT_PREAPPROVED) Then 'Y' Else  if Trim(Src_to_Xfm.LIMIT_AMOUNT_PREAPPROVED) = 0 Then 'Y' Else  if trim(Src_to_Xfm.LIMIT_AMOUNT_PREAPPROVED) = '' then 'Y' ELSE 'N',
		IFF({{ ref('CSE_CPO_BUS_APP_PROD') }}.LIMIT_AMOUNT_PREAPPROVED IS NULL, 'Y', IFF(TRIM({{ ref('CSE_CPO_BUS_APP_PROD') }}.LIMIT_AMOUNT_PREAPPROVED) = 0, 'Y', IFF(TRIM({{ ref('CSE_CPO_BUS_APP_PROD') }}.LIMIT_AMOUNT_PREAPPROVED) = '', 'Y', 'N'))) AS svIsNullLimitAmountPreApproval,
		-- *SRC*: \(20)if IsNull(Src_to_Xfm.APP_PROD_ID) Then 'Y' Else  if trim(Src_to_Xfm.APP_PROD_ID) = '' then 'Y' ELSE  if trim(Src_to_Xfm.APP_PROD_ID) = 0 Then 'Y' Else 'N',
		IFF({{ ref('CSE_CPO_BUS_APP_PROD') }}.APP_PROD_ID IS NULL, 'Y', IFF(TRIM({{ ref('CSE_CPO_BUS_APP_PROD') }}.APP_PROD_ID) = '', 'Y', IFF(TRIM({{ ref('CSE_CPO_BUS_APP_PROD') }}.APP_PROD_ID) = 0, 'Y', 'N'))) AS svIsNullAppProdId,
		-- *SRC*: \(20)if IsNull(Src_to_Xfm.LIMIT_AMOUNT_REQUESTED) Then 'Y' Else  if Trim(Src_to_Xfm.LIMIT_AMOUNT_REQUESTED) = 0 Then 'Y' Else  if trim(Src_to_Xfm.LIMIT_AMOUNT_REQUESTED) = '' then 'Y' ELSE 'N',
		IFF({{ ref('CSE_CPO_BUS_APP_PROD') }}.LIMIT_AMOUNT_REQUESTED IS NULL, 'Y', IFF(TRIM({{ ref('CSE_CPO_BUS_APP_PROD') }}.LIMIT_AMOUNT_REQUESTED) = 0, 'Y', IFF(TRIM({{ ref('CSE_CPO_BUS_APP_PROD') }}.LIMIT_AMOUNT_REQUESTED) = '', 'Y', 'N'))) AS svIsNullLimitAmountRequested,
		-- *SRC*: \(20)if IsNull(Src_to_Xfm.LIMIT_AMOUNT_APPROVED) Then 'Y' Else  if Trim(Src_to_Xfm.LIMIT_AMOUNT_APPROVED) = 0 Then 'Y' Else  if trim(Src_to_Xfm.LIMIT_AMOUNT_APPROVED) = '' then 'Y' ELSE 'N',
		IFF({{ ref('CSE_CPO_BUS_APP_PROD') }}.LIMIT_AMOUNT_APPROVED IS NULL, 'Y', IFF(TRIM({{ ref('CSE_CPO_BUS_APP_PROD') }}.LIMIT_AMOUNT_APPROVED) = 0, 'Y', IFF(TRIM({{ ref('CSE_CPO_BUS_APP_PROD') }}.LIMIT_AMOUNT_APPROVED) = '', 'Y', 'N'))) AS svIsNullLimitAmountApproval,
		-- *SRC*: \(20)if IsNull(Src_to_Xfm.EXISTING_ACCOUNT_LIMIT) Then 'Y' Else  if Trim(Src_to_Xfm.EXISTING_ACCOUNT_LIMIT) = 0 Then 'Y' Else  if trim(Src_to_Xfm.EXISTING_ACCOUNT_LIMIT) = '' then 'Y' ELSE 'N',
		IFF({{ ref('CSE_CPO_BUS_APP_PROD') }}.EXISTING_ACCOUNT_LIMIT IS NULL, 'Y', IFF(TRIM({{ ref('CSE_CPO_BUS_APP_PROD') }}.EXISTING_ACCOUNT_LIMIT) = 0, 'Y', IFF(TRIM({{ ref('CSE_CPO_BUS_APP_PROD') }}.EXISTING_ACCOUNT_LIMIT) = '', 'Y', 'N'))) AS svIsNullExistingAccountLimit,
		-- *SRC*: 'CSEPO' : Src_to_Xfm.APP_PROD_ID,
		CONCAT('CSEPO', {{ ref('CSE_CPO_BUS_APP_PROD') }}.APP_PROD_ID) AS APPT_PDCT_I,
		'LAAP' AS AMT_TYPE_C,
		'AUD' AS CNCY_C,
		{{ ref('CSE_CPO_BUS_APP_PROD') }}.LIMIT_AMOUNT_APPROVED AS APPT_PDCT_A,
		-- *SRC*: SetNull(),
		SETNULL() AS XCES_AMT_REAS_X,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS EFFT_D,
		'CSE' AS SRCE_SYST_C,
		-- *SRC*: StringToDate('9999-12-31', "%yyyy-%mm-%dd"),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		pRUN_STRM_C AS RUN_STRM,
		'A' AS DLTA_VERS
	FROM {{ ref('CSE_CPO_BUS_APP_PROD') }}
	WHERE svIsNullLimitAmountApproval = 'N' AND svIsNullAppProdId = 'N'
)

SELECT * FROM Xfm__Laap