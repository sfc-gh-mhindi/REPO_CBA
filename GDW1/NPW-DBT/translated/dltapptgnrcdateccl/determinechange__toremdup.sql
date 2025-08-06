{{ config(materialized='view', tags=['DltApptGnrcDateCCL']) }}

WITH DetermineChange__ToRemDup AS (
	SELECT
		APPT_I,
		DATE_ROLE_C,
		EFFT_D,
		-- *SRC*: \(20)IF IsNull(ToTrans.GNRC_ROLE_S) THEN SetNull() ELSE ToTrans.GNRC_ROLE_S,
		IFF({{ ref('Funnel') }}.GNRC_ROLE_S IS NULL, SETNULL(), {{ ref('Funnel') }}.GNRC_ROLE_S) AS GNRC_ROLE_S,
		-- *SRC*: \(20)IF IsNull(ToTrans.GNRC_ROLE_D) THEN SetNull() ELSE ToTrans.GNRC_ROLE_D,
		IFF({{ ref('Funnel') }}.GNRC_ROLE_D IS NULL, SETNULL(), {{ ref('Funnel') }}.GNRC_ROLE_D) AS GNRC_ROLE_D,
		-- *SRC*: \(20)IF IsNull(ToTrans.GNRC_ROLE_T) THEN SetNull() ELSE ToTrans.GNRC_ROLE_T,
		IFF({{ ref('Funnel') }}.GNRC_ROLE_T IS NULL, SETNULL(), {{ ref('Funnel') }}.GNRC_ROLE_T) AS GNRC_ROLE_T,
		PROS_KEY_EFFT_I,
		EROR_SEQN_I,
		-- *SRC*: \(20)IF IsNull(ToTrans.MODF_S) THEN SetNull() ELSE ToTrans.MODF_S,
		IFF({{ ref('Funnel') }}.MODF_S IS NULL, SETNULL(), {{ ref('Funnel') }}.MODF_S) AS MODF_S,
		-- *SRC*: \(20)IF IsNull(ToTrans.MODF_D) THEN SetNull() ELSE ToTrans.MODF_D,
		IFF({{ ref('Funnel') }}.MODF_D IS NULL, SETNULL(), {{ ref('Funnel') }}.MODF_D) AS MODF_D,
		-- *SRC*: \(20)IF IsNull(ToTrans.MODF_T) THEN SetNull() ELSE ToTrans.MODF_T,
		IFF({{ ref('Funnel') }}.MODF_T IS NULL, SETNULL(), {{ ref('Funnel') }}.MODF_T) AS MODF_T,
		USER_I,
		CHNG_REAS_TYPE_C,
		-- *SRC*: \(20)If ToTrans.MODF_S = ToTrans.MODF_S_TEMP Then StringToDate("99991231", "%yyyy%mm%dd") Else StringToDate(pRUN_STRM_PROS_D, "%yyyy%mm%dd"),
		IFF({{ ref('Funnel') }}.MODF_S = {{ ref('Funnel') }}.MODF_S_TEMP, STRINGTODATE('99991231', '%yyyy%mm%dd'), STRINGTODATE(pRUN_STRM_PROS_D, '%yyyy%mm%dd')) AS EXPY_D,
		-- *SRC*: \(20)If ToTrans.MODF_S = ToTrans.MODF_S_TEMP Then SetNull() Else pGDW_PROS_ID,
		IFF({{ ref('Funnel') }}.MODF_S = {{ ref('Funnel') }}.MODF_S_TEMP, SETNULL(), pGDW_PROS_ID) AS PROS_KEY_EXPY_I
	FROM {{ ref('Funnel') }}
	WHERE {{ ref('Funnel') }}.change_code = 1 OR {{ ref('Funnel') }}.change_code = 3
)

SELECT * FROM DetermineChange__ToRemDup