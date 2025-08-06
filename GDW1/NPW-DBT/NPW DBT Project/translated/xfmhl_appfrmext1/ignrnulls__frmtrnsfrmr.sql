{{ config(materialized='view', tags=['XfmHL_APPFrmExt1']) }}

WITH IgnrNulls__FrmTrnsfrmr AS (
	SELECT
		-- *SRC*: \(20)IF IsNull(ToignrNulls1.CHL_APP_HL_APP_ID) THEN "N" ELSE  IF Trim(ToignrNulls1.CHL_APP_HL_APP_ID) = '' THEN "N" ELSE "Y",
		IFF({{ ref('SrcChlBusApp') }}.CHL_APP_HL_APP_ID IS NULL, 'N', IFF(TRIM({{ ref('SrcChlBusApp') }}.CHL_APP_HL_APP_ID) = '', 'N', 'Y')) AS Svcon,
		-- *SRC*: \(20)IF IsNull(ToignrNulls1.EXEC_DOCUMENTS_RECEIVER_TYPE) THEN "N" ELSE  IF Trim(ToignrNulls1.EXEC_DOCUMENTS_RECEIVER_TYPE) = '' THEN "N" ELSE "Y",
		IFF({{ ref('SrcChlBusApp') }}.EXEC_DOCUMENTS_RECEIVER_TYPE IS NULL, 'N', IFF(TRIM({{ ref('SrcChlBusApp') }}.EXEC_DOCUMENTS_RECEIVER_TYPE) = '', 'N', 'Y')) AS SvlsValidRecord,
		-- *SRC*: \(20)If IsNull(ToignrNulls1.REPRINT_COUNT) THEN "N" ELSE  IF Len(trim(ToignrNulls1.REPRINT_COUNT)) = 0 THEN "N" ELSE  If Trim(( IF IsNotNull((ToignrNulls1.REPRINT_COUNT)) THEN (ToignrNulls1.REPRINT_COUNT) ELSE 0)) = 0 THEN "N" ELSE "Y",
		IFF({{ ref('SrcChlBusApp') }}.REPRINT_COUNT IS NULL, 'N', IFF(LEN(TRIM({{ ref('SrcChlBusApp') }}.REPRINT_COUNT)) = 0, 'N', IFF(TRIM(IFF({{ ref('SrcChlBusApp') }}.REPRINT_COUNT IS NOT NULL, {{ ref('SrcChlBusApp') }}.REPRINT_COUNT, 0)) = 0, 'N', 'Y'))) AS SvlsValidRecord1,
		-- *SRC*: 'CSEHL' : ( IF IsNotNull((ToignrNulls1.CHL_APP_HL_APP_ID)) THEN (ToignrNulls1.CHL_APP_HL_APP_ID) ELSE ('55555')),
		CONCAT('CSEHL', IFF({{ ref('SrcChlBusApp') }}.CHL_APP_HL_APP_ID IS NOT NULL, {{ ref('SrcChlBusApp') }}.CHL_APP_HL_APP_ID, '55555')) AS SvApptI,
		-- *SRC*: \(20)If AsInteger(Trim(( IF IsNotNull((ToignrNulls1.CHL_APP_HL_APP_ID)) THEN (ToignrNulls1.CHL_APP_HL_APP_ID) ELSE 0))) = 0 THEN "N" ELSE "Y",
		IFF(ASINTEGER(TRIM(IFF({{ ref('SrcChlBusApp') }}.CHL_APP_HL_APP_ID IS NOT NULL, {{ ref('SrcChlBusApp') }}.CHL_APP_HL_APP_ID, 0))) = 0, 'N', 'Y') AS SvAppizero,
		SvApptI AS APPT_I,
		{{ ref('SrcChlBusApp') }}.REPRINT_COUNT AS APPT_ACTV_Q,
		RUN_STREAM AS RUN_STRM
	FROM {{ ref('SrcChlBusApp') }}
	WHERE Svcon = 'Y' AND SvlsValidRecord1 = 'Y' AND SvAppizero = 'Y'
)

SELECT * FROM IgnrNulls__FrmTrnsfrmr