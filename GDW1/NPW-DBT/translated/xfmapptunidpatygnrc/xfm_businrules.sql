{{ config(materialized='view', tags=['XfmApptUnidPatyGnrc']) }}

WITH Xfm_BusinRules AS (
	SELECT
		-- *SRC*: \(20)If (IsNull(Out_SrcCseChlBusAppComDet.REFERRAL_AGENT_ID) Or Len(Trim(Out_SrcCseChlBusAppComDet.REFERRAL_AGENT_ID, '0')) = 0 Or Trim(Out_SrcCseChlBusAppComDet.REFERRAL_AGENT_ID) = '') Then 'N' Else 'Y',
		IFF({{ ref('SrcCseChlBusAppCommDet') }}.REFERRAL_AGENT_ID IS NULL OR LEN(TRIM({{ ref('SrcCseChlBusAppCommDet') }}.REFERRAL_AGENT_ID, '0')) = 0 OR TRIM({{ ref('SrcCseChlBusAppCommDet') }}.REFERRAL_AGENT_ID) = '', 'N', 'Y') AS RefId,
		-- *SRC*: \(20)If (IsNull(Out_SrcCseChlBusAppComDet.HL_APP_ID) Or Len(Trim(Out_SrcCseChlBusAppComDet.HL_APP_ID, '0')) = 0 Or Trim(Out_SrcCseChlBusAppComDet.HL_APP_ID) = '') Then 'N' Else 'Y',
		IFF({{ ref('SrcCseChlBusAppCommDet') }}.HL_APP_ID IS NULL OR LEN(TRIM({{ ref('SrcCseChlBusAppCommDet') }}.HL_APP_ID, '0')) = 0 OR TRIM({{ ref('SrcCseChlBusAppCommDet') }}.HL_APP_ID) = '', 'N', 'Y') AS AppId,
		-- *SRC*: 'CSEHL' : Out_SrcCseChlBusAppComDet.HL_APP_ID,
		CONCAT('CSEHL', {{ ref('SrcCseChlBusAppCommDet') }}.HL_APP_ID) AS APPT_I,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		'9999-12-31' AS EXPY_D,
		'REFR' AS REL_TYPE_C,
		'UNKN' AS REL_REAS_C,
		'U' AS REL_STUS_C,
		-- *SRC*: SetNull(),
		SETNULL() AS REL_LEVL_C,
		'CSE' AS SRCE_SYST_C,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: 'CSEA3' : Out_SrcCseChlBusAppComDet.REFERRAL_AGENT_ID,
		CONCAT('CSEA3', {{ ref('SrcCseChlBusAppCommDet') }}.REFERRAL_AGENT_ID) AS UNID_PATY_I,
		'0' AS ROW_SECU_ACCS_C
	FROM {{ ref('SrcCseChlBusAppCommDet') }}
	WHERE RefId = 'Y' OR AppId = 'Y'
)

SELECT * FROM Xfm_BusinRules