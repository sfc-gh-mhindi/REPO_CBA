{{ config(materialized='view', tags=['DltCSE_APPT_HL_COND_APPR_FrmTMPAPPTHLCONDAPPR_HSCA']) }}

WITH XfmCheckDeltaAction__OutTgtApptHlCondApprUpdate AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		{{ ref('SrcTmpApptHlCondApprTera') }}.NEW_APPT_I AS APPT_I,
		{{ ref('SrcTmpApptHlCondApprTera') }}.OLD_EFFT_D AS EFFT_D,
		-- *SRC*: \(20)If InTmpApptHlCondApprTera.OLD_EFFT_D < StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd') Then DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')) Else StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		IFF({{ ref('SrcTmpApptHlCondApprTera') }}.OLD_EFFT_D < STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd'), DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')), STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('SrcTmpApptHlCondApprTera') }}
	WHERE {{ ref('SrcTmpApptHlCondApprTera') }}.REC_TYPE = 'U'
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptHlCondApprUpdate