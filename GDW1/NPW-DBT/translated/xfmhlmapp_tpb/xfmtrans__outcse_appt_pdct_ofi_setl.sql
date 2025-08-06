{{ config(materialized='view', tags=['XfmHlmapp_Tpb']) }}

WITH XfmTrans__outCSE_APPT_PDCT_OFI_SETL AS (
	SELECT
		-- *SRC*: \(20)IF IsNull(Totrns.CHL_AGENT_ALIAS_ID) THEN "N" ELSE  IF Trim(Totrns.CHL_AGENT_ALIAS_ID) = '' THEN "N" ELSE "Y",
		IFF({{ ref('MergeDS') }}.CHL_AGENT_ALIAS_ID IS NULL, 'N', IFF(TRIM({{ ref('MergeDS') }}.CHL_AGENT_ALIAS_ID) = '', 'N', 'Y')) AS svIsValidrecord1,
		-- *SRC*: \(20)IF IsNull(Totrns.HLM_APP_DISCHARGE_REASON_ID) THEN "N" ELSE  If Trim(Totrns.HLM_APP_DISCHARGE_REASON_ID) = '' THEN "N" ELSE "Y",
		IFF({{ ref('MergeDS') }}.HLM_APP_DISCHARGE_REASON_ID IS NULL, 'N', IFF(TRIM({{ ref('MergeDS') }}.HLM_APP_DISCHARGE_REASON_ID) = '', 'N', 'Y')) AS svIsValidrecord2,
		-- *SRC*: \(20)IF IsNull(Totrns.HLM_APP_PROD_ID) THEN "N" ELSE  If Trim(Totrns.HLM_APP_PROD_ID) = '' THEN "N" ELSE "Y",
		IFF({{ ref('MergeDS') }}.HLM_APP_PROD_ID IS NULL, 'N', IFF(TRIM({{ ref('MergeDS') }}.HLM_APP_PROD_ID) = '', 'N', 'Y')) AS svIsValidrecord3,
		-- *SRC*: \(20)If (Trim(( IF IsNotNull((Totrns.HLM_APP_PROD_ID)) THEN (Totrns.HLM_APP_PROD_ID) ELSE 0)) = 0) Then 'N' else  if Trim(Totrns.HLM_APP_PROD_ID) = '' Then 'N' else  if num(Totrns.HLM_APP_PROD_ID) then ( if (StringToDecimal(TRIM(Totrns.HLM_APP_PROD_ID)) = 0) Then 'N' else 'Y') else 'Y',
		IFF(TRIM(IFF({{ ref('MergeDS') }}.HLM_APP_PROD_ID IS NOT NULL, {{ ref('MergeDS') }}.HLM_APP_PROD_ID, 0)) = 0, 'N', IFF(TRIM({{ ref('MergeDS') }}.HLM_APP_PROD_ID) = '', 'N', IFF(NUM({{ ref('MergeDS') }}.HLM_APP_PROD_ID), IFF(STRINGTODECIMAL(TRIM({{ ref('MergeDS') }}.HLM_APP_PROD_ID)) = 0, 'N', 'Y'), 'Y'))) AS svIsValidrecord4,
		-- *SRC*: \(20)If (Trim(( IF IsNotNull((Totrns.PEXA_FLAG)) THEN (Totrns.PEXA_FLAG) ELSE 0)) = 0) Then 'N' else  if Trim(Totrns.PEXA_FLAG) = '' Then 'N' else  if num(Totrns.PEXA_FLAG) then ( if (StringToDecimal(TRIM(Totrns.PEXA_FLAG)) = 0) Then 'N' else 'Y') else 'Y',
		IFF(TRIM(IFF({{ ref('MergeDS') }}.PEXA_FLAG IS NOT NULL, {{ ref('MergeDS') }}.PEXA_FLAG, 0)) = 0, 'N', IFF(TRIM({{ ref('MergeDS') }}.PEXA_FLAG) = '', 'N', IFF(NUM({{ ref('MergeDS') }}.PEXA_FLAG), IFF(STRINGTODECIMAL(TRIM({{ ref('MergeDS') }}.PEXA_FLAG)) = 0, 'N', 'Y'), 'Y'))) AS svlsValidPexachk,
		-- *SRC*: "CSEHM" : Totrns.HLM_APP_PROD_ID,
		CONCAT('CSEHM', {{ ref('MergeDS') }}.HLM_APP_PROD_ID) AS APPT_PDCT_I,
		{{ ref('MergeDS') }}.OFI_ID AS OFI_IDNN_X,
		-- *SRC*: \(20)If IsNull(Totrns.OFI_NAME) THEN SETNULL() ELSE  IF TRIM(Totrns.OFI_NAME) = '' THEN SETNULL() ELSE  if num(trim(Totrns.OFI_NAME)) then ( if StringToDecimal(Totrns.OFI_NAME) = 0 then SETnull() else Totrns.OFI_NAME) Else Totrns.OFI_NAME,
		IFF({{ ref('MergeDS') }}.OFI_NAME IS NULL, SETNULL(), IFF(TRIM({{ ref('MergeDS') }}.OFI_NAME) = '', SETNULL(), IFF(NUM(TRIM({{ ref('MergeDS') }}.OFI_NAME)), IFF(STRINGTODECIMAL({{ ref('MergeDS') }}.OFI_NAME) = 0, SETNULL(), {{ ref('MergeDS') }}.OFI_NAME), {{ ref('MergeDS') }}.OFI_NAME))) AS OFI_M,
		-- *SRC*: StringToDate(Trim(ETL_PROCESS_DT[1, 4]) : '-' : Trim(ETL_PROCESS_DT[5, 2]) : '-' : Trim(ETL_PROCESS_DT[7, 2]), "%yyyy-%mm-%dd"),
		STRINGTODATE(CONCAT(CONCAT(CONCAT(CONCAT(TRIM(SUBSTRING(ETL_PROCESS_DT, 1, 4)), '-'), TRIM(SUBSTRING(ETL_PROCESS_DT, 5, 2))), '-'), TRIM(SUBSTRING(ETL_PROCESS_DT, 7, 2))), '%yyyy-%mm-%dd') AS EFFT_D,
		-- *SRC*: StringToDate("99991231", "%yyyy%mm%dd"),
		STRINGTODATE('99991231', '%yyyy%mm%dd') AS EXPY_D,
		0 AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		0 AS ROW_SECU_ACCS_C,
		RUN_STREAM AS RUN_STREAM
	FROM {{ ref('MergeDS') }}
	WHERE svIsValidrecord4 = 'Y'
)

SELECT * FROM XfmTrans__outCSE_APPT_PDCT_OFI_SETL