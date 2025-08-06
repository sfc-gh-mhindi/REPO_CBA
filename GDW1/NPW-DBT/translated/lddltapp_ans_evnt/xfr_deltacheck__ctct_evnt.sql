{{ config(materialized='view', tags=['LdDltAPP_ANS_EVNT']) }}

WITH XFR_DeltaCheck__CTCT_Evnt AS (
	SELECT
		-- *SRC*: \(20)If To_XFR_NullToEmpty.TGT_EVNT_I = "RE33" Then "L" Else "DL",
		IFF({{ ref('Src_Tera_XS_EVNT') }}.TGT_EVNT_I = 'RE33', 'L', 'DL') AS INSERT,
		-- *SRC*: \(20)If Len(Trim(To_XFR_NullToEmpty.CIF_CODE)) < 10 and Len(Trim(To_XFR_NullToEmpty.CIF_CODE)) > 0 THEN Str('0', 10 - Len(To_XFR_NullToEmpty.CIF_CODE)) : To_XFR_NullToEmpty.CIF_CODE ELSE To_XFR_NullToEmpty.CIF_CODE,
		IFF(LEN(TRIM({{ ref('Src_Tera_XS_EVNT') }}.CIF_CODE)) < 10 AND LEN(TRIM({{ ref('Src_Tera_XS_EVNT') }}.CIF_CODE)) > 0, CONCAT(STR('0', 10 - LEN({{ ref('Src_Tera_XS_EVNT') }}.CIF_CODE)), {{ ref('Src_Tera_XS_EVNT') }}.CIF_CODE), {{ ref('Src_Tera_XS_EVNT') }}.CIF_CODE) AS PATY,
		-- *SRC*: \(20)If Len(( IF IsNotNull((To_XFR_NullToEmpty.CIF_CODE)) THEN (To_XFR_NullToEmpty.CIF_CODE) ELSE "")) > '0' Then 'Y' Else 'N',
		IFF(LEN(IFF({{ ref('Src_Tera_XS_EVNT') }}.CIF_CODE IS NOT NULL, {{ ref('Src_Tera_XS_EVNT') }}.CIF_CODE, '')) > '0', 'Y', 'N') AS PATYCOND,
		-- *SRC*: \(20)If To_XFR_NullToEmpty.APPT_QLFY_C = 'HL' Then '3134' Else '3135',
		IFF({{ ref('Src_Tera_XS_EVNT') }}.APPT_QLFY_C = 'HL', '3134', '3135') AS CODE,
		{{ ref('Src_Tera_XS_EVNT') }}.SRC_EVNT_I AS EVNT_I,
		{{ ref('Src_Tera_XS_EVNT') }}.APPL_ID AS SRCE_SYST_EVNT_I,
		-- *SRC*: TimestampToDate(To_XFR_NullToEmpty.MOD_TIMESTAMP),
		TIMESTAMPTODATE({{ ref('Src_Tera_XS_EVNT') }}.MOD_TIMESTAMP) AS EVNT_ACTL_D,
		'CSE' AS SRCE_SYST_C,
		CTCTEVNTPK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		-- *SRC*: SetNull(),
		SETNULL() AS SRCE_SYST_EVNT_TYPE_I,
		'UNKN' AS CTCT_EVNT_TYPE_C,
		-- *SRC*: TimestampToTime(To_XFR_NullToEmpty.MOD_TIMESTAMP),
		TIMESTAMPTOTIME({{ ref('Src_Tera_XS_EVNT') }}.MOD_TIMESTAMP) AS EVNT_ACTL_T,
		'0' AS ROW_SECU_ACCS_C
	FROM {{ ref('Src_Tera_XS_EVNT') }}
	WHERE INSERT = 'L'
)

SELECT * FROM XFR_DeltaCheck__CTCT_Evnt