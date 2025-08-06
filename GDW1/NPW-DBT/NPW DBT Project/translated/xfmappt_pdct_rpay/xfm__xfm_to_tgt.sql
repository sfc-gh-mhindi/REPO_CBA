{{ config(materialized='view', tags=['XfmAppt_Pdct_Rpay']) }}

WITH Xfm__Xfm_to_Tgt AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToXfm.RPAY_SRCE_C) Then 'Y' Else  If Trim(ToXfm.RPAY_SRCE_C) = '' Then 'Y' Else 'N',
		IFF({{ ref('Lkp') }}.RPAY_SRCE_C IS NULL, 'Y', IFF(TRIM({{ ref('Lkp') }}.RPAY_SRCE_C) = '', 'Y', 'N')) AS svIsNullRpaySrceC,
		-- *SRC*: \(20)If IsNull(ToXfm.PO_REPAYMENT_SOURCE_OTHER) Then 'Y' Else  if Trim(ToXfm.PO_REPAYMENT_SOURCE_OTHER) = '' Then 'Y' Else  if Trim(ToXfm.PO_REPAYMENT_SOURCE_OTHER) = 0 Then 'Y' Else 'N',
		IFF({{ ref('Lkp') }}.PO_REPAYMENT_SOURCE_OTHER IS NULL, 'Y', IFF(TRIM({{ ref('Lkp') }}.PO_REPAYMENT_SOURCE_OTHER) = '', 'Y', IFF(TRIM({{ ref('Lkp') }}.PO_REPAYMENT_SOURCE_OTHER) = 0, 'Y', 'N'))) AS svIsNullPoReypaymentSourceOther,
		-- *SRC*: "CSEPO" : Trim(ToXfm.APP_PROD_ID),
		CONCAT('CSEPO', TRIM({{ ref('Lkp') }}.APP_PROD_ID)) AS APPT_PDCT_I,
		'U' AS RPAY_TYPE_C,
		-- *SRC*: StringToDate(Trim(pRUN_STRM_PROS_D[1, 4]) : '-' : Trim(pRUN_STRM_PROS_D[5, 2]) : '-' : Trim(pRUN_STRM_PROS_D[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(pRUN_STRM_PROS_D, 1, 4)), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 5, 2))), '-'), TRIM(SUBSTRING(pRUN_STRM_PROS_D, 7, 2))), '%yyyy-%mm-%dd') AS EFFT_D,
		'U' AS PAYT_FREQ_C,
		-- *SRC*: SetNull(),
		SETNULL() AS STRT_RPAY_D,
		-- *SRC*: StringToDate('9999-12-31', "%yyyy-%mm-%dd"),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		'CSE' AS SRCE_SYST_C,
		-- *SRC*: \(20)If svIsNullRpaySrceC = 'N' Then ToXfm.RPAY_SRCE_C Else 9999,
		IFF(svIsNullRpaySrceC = 'N', {{ ref('Lkp') }}.RPAY_SRCE_C, 9999) AS RPAY_SRCE_C,
		-- *SRC*: \(20)If svIsNullPoReypaymentSourceOther = 'Y' Then SetNull() Else ToXfm.PO_REPAYMENT_SOURCE_OTHER,
		IFF(svIsNullPoReypaymentSourceOther = 'Y', SETNULL(), {{ ref('Lkp') }}.PO_REPAYMENT_SOURCE_OTHER) AS RPAY_SRCE_OTHR_X,
		pRUN_STRM_C AS RUN_STRM
	FROM {{ ref('Lkp') }}
	WHERE 
)

SELECT * FROM Xfm__Xfm_to_Tgt