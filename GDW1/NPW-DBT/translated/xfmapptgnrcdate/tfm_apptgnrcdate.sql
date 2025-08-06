{{ config(materialized='view', tags=['XfmApptGnrcDate']) }}

WITH Tfm_apptGnrcDate AS (
	SELECT
		-- *SRC*: \(20)If IsNull(ToTrfrmrTemp.EVNT_I) Then 'N' Else  If Trim(ToTrfrmrTemp.EVNT_I) = '' Then 'N' Else 'Y',
		IFF({{ ref('ApptGnrcDtToTfrmr') }}.EVNT_I IS NULL, 'N', IFF(TRIM({{ ref('ApptGnrcDtToTfrmr') }}.EVNT_I) = '', 'N', 'Y')) AS svIsHLmAppIdNullOrBlnk,
		-- *SRC*: \(20)If IsNull(ToTrfrmrTemp.VALU_D) Then 'N' Else  If Trim(ToTrfrmrTemp.VALU_D) = '' Then 'N' Else 'Y',
		IFF({{ ref('ApptGnrcDtToTfrmr') }}.VALU_D IS NULL, 'N', IFF(TRIM({{ ref('ApptGnrcDtToTfrmr') }}.VALU_D) = '', 'N', 'Y')) AS svIsValuDNullOrBlnk,
		-- *SRC*: DateToString(ToTrfrmrTemp.VALU_D, "%yyyy-%mm-%dd"),
		DATETOSTRING({{ ref('ApptGnrcDtToTfrmr') }}.VALU_D, '%yyyy-%mm-%dd') AS svValueD,
		-- *SRC*: TimeToString(ToTrfrmrTemp.VALU_T, "%hh:%nn:%ss"),
		TIMETOSTRING({{ ref('ApptGnrcDtToTfrmr') }}.VALU_T, '%hh:%nn:%ss') AS svValueT,
		APPT_I,
		'SDBK' AS DATE_ROLE_C,
		-- *SRC*: StringtoDate(ETL_PROCESS_DT, "%yyyy%mm%dd"),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToTimestamp(ToTrfrmrTemp.VALU_D[1, 4] : '-' : ToTrfrmrTemp.VALU_D[6, 2] : '-' : ToTrfrmrTemp.VALU_D[9, 2] : ' ' : ToTrfrmrTemp.VALU_T[1, 2] : ':' : ToTrfrmrTemp.VALU_T[4, 2] : ':' : ToTrfrmrTemp.VALU_T[7, 2], "%yyyy-%mm-%dd %hh:%nn:%ss"),
		STRINGTOTIMESTAMP(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(CONCAT(SUBSTRING({{ ref('ApptGnrcDtToTfrmr') }}.VALU_D, 1, 4), '-'), SUBSTRING({{ ref('ApptGnrcDtToTfrmr') }}.VALU_D, 6, 2)), '-'), SUBSTRING({{ ref('ApptGnrcDtToTfrmr') }}.VALU_D, 9, 2)), ' '), SUBSTRING({{ ref('ApptGnrcDtToTfrmr') }}.VALU_T, 1, 2)), ':'), SUBSTRING({{ ref('ApptGnrcDtToTfrmr') }}.VALU_T, 4, 2)), ':'), SUBSTRING({{ ref('ApptGnrcDtToTfrmr') }}.VALU_T, 7, 2)), '%yyyy-%mm-%dd %hh:%nn:%ss') AS GNRC_ROLE_S,
		-- *SRC*: StringToDate(ToTrfrmrTemp.VALU_D, "%yyyy-%mm-%dd"),
		STRINGTODATE({{ ref('ApptGnrcDtToTfrmr') }}.VALU_D, '%yyyy-%mm-%dd') AS GNRC_ROLE_D,
		-- *SRC*: StringToTime(ToTrfrmrTemp.VALU_T[1, 2] : ':' : ToTrfrmrTemp.VALU_T[4, 2] : ':' : ToTrfrmrTemp.VALU_T[7, 2], "%hh:%nn:%ss"),
		STRINGTOTIME(CONCAT(CONCAT(CONCAT(CONCAT(SUBSTRING({{ ref('ApptGnrcDtToTfrmr') }}.VALU_T, 1, 2), ':'), SUBSTRING({{ ref('ApptGnrcDtToTfrmr') }}.VALU_T, 4, 2)), ':'), SUBSTRING({{ ref('ApptGnrcDtToTfrmr') }}.VALU_T, 7, 2)), '%hh:%nn:%ss') AS GNRC_ROLE_T,
		'9999-12-31' AS EXPY_D,
		'' AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I,
		-- *SRC*: SetNull(),
		SETNULL() AS MODF_S,
		-- *SRC*: SetNull(),
		SETNULL() AS MODF_D,
		-- *SRC*: SetNull(),
		SETNULL() AS MODF_T,
		-- *SRC*: SetNull(),
		SETNULL() AS USER_I,
		-- *SRC*: SetNull(),
		SETNULL() AS CHNG_REAS_TYPE_C
	FROM {{ ref('ApptGnrcDtToTfrmr') }}
	WHERE svIsHLmAppIdNullOrBlnk = 'Y' AND svIsValuDNullOrBlnk = 'Y'
)

SELECT * FROM Tfm_apptGnrcDate