{{ config(materialized='view', tags=['XfmUnidPatyNameGnrc']) }}

WITH Xfm_BusinRules AS (
	SELECT
		-- *SRC*: \(20)If Len(Trim(Out_SrcCseChlBusAppCommDet.REFERRAL_AGENT_ID, '0')) = 0 Then 'N' else  if Trim(Out_SrcCseChlBusAppCommDet.REFERRAL_AGENT_ID) = '' Then 'N' Else  If Out_SrcCseChlBusAppCommDet.REFERRAL_AGENT_ID = '99999' Then 'N' Else 'Y',
		IFF(LEN(TRIM({{ ref('Modify_Null') }}.REFERRAL_AGENT_ID, '0')) = 0, 'N', IFF(TRIM({{ ref('Modify_Null') }}.REFERRAL_AGENT_ID) = '', 'N', IFF({{ ref('Modify_Null') }}.REFERRAL_AGENT_ID = '99999', 'N', 'Y'))) AS svRefAgntId,
		-- *SRC*: \(20)If Len(Trim(Out_SrcCseChlBusAppCommDet.HL_APP_ID, '0')) = 0 Then 'N' else  if Trim(Out_SrcCseChlBusAppCommDet.HL_APP_ID) = '' Then 'N' Else  If Out_SrcCseChlBusAppCommDet.HL_APP_ID = '99999' Then 'N' Else 'Y',
		IFF(LEN(TRIM({{ ref('Modify_Null') }}.HL_APP_ID, '0')) = 0, 'N', IFF(TRIM({{ ref('Modify_Null') }}.HL_APP_ID) = '', 'N', IFF({{ ref('Modify_Null') }}.HL_APP_ID = '99999', 'N', 'Y'))) AS svHlAppId,
		-- *SRC*: \(20)If Len(Trim(Out_SrcCseChlBusAppCommDet.NAME, '0')) = 0 Then 'N' else  if Trim(Out_SrcCseChlBusAppCommDet.NAME) = '' Then 'N' Else  If Out_SrcCseChlBusAppCommDet.NAME = '99999' Then 'N' Else 'Y',
		IFF(LEN(TRIM({{ ref('Modify_Null') }}.NAME, '0')) = 0, 'N', IFF(TRIM({{ ref('Modify_Null') }}.NAME) = '', 'N', IFF({{ ref('Modify_Null') }}.NAME = '99999', 'N', 'Y'))) AS svName,
		-- *SRC*: "CSEA3" : Out_SrcCseChlBusAppCommDet.REFERRAL_AGENT_ID,
		CONCAT('CSEA3', {{ ref('Modify_Null') }}.REFERRAL_AGENT_ID) AS UNID_PATY_I,
		'UNKN' AS PATY_NAME_TYPE_C,
		-- *SRC*: StringToDate(pRUN_STRM_PROS_D, "%yyyy%mm%dd"),
		STRINGTODATE(pRUN_STRM_PROS_D, '%yyyy%mm%dd') AS EFFT_D,
		'CSE' AS SRCE_SYST_C,
		'UNKN' AS SALU_C,
		'TPRF' AS PATY_ROLE_C,
		'N/A' AS TITL_C,
		-- *SRC*: SetNull(),
		SETNULL() AS FRST_M,
		-- *SRC*: SetNull(),
		SETNULL() AS SCND_M,
		{{ ref('Modify_Null') }}.NAME AS SRNM_M,
		-- *SRC*: SetNull(),
		SETNULL() AS THRD_M,
		-- *SRC*: SetNull(),
		SETNULL() AS FRTH_M,
		'UNKN' AS SUF_C,
		'9999-12-31' AS EXPY_D,
		pGDW_PROS_ID AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		-- *SRC*: SetNull(),
		SETNULL() AS CO_CTCT_FRST_M,
		-- *SRC*: SetNull(),
		SETNULL() AS CO_CTCT_LAST_M,
		-- *SRC*: SetNull(),
		SETNULL() AS CO_CTCT_PRFR_M
	FROM {{ ref('Modify_Null') }}
	WHERE svHlAppId = 'Y' AND svName = 'Y' AND svRefAgntId = 'Y'
)

SELECT * FROM Xfm_BusinRules