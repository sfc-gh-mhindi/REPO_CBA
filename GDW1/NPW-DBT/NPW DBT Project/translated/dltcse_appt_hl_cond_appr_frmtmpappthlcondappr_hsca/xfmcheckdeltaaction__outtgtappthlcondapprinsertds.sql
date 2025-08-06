{{ config(materialized='view', tags=['DltCSE_APPT_HL_COND_APPR_FrmTMPAPPTHLCONDAPPR_HSCA']) }}

WITH XfmCheckDeltaAction__OutTgtApptHlCondApprInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		{{ ref('SrcTmpApptHlCondApprTera') }}.NEW_APPT_I AS APPT_I,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		{{ ref('SrcTmpApptHlCondApprTera') }}.NEW_COND_APPR_F AS COND_APPR_F,
		{{ ref('SrcTmpApptHlCondApprTera') }}.NEW_COND_APPR_CONV_TO_FULL_D AS COND_APPR_CONV_TO_FULL_D,
		'9999-12-31' AS EXPY_D,
		0 AS ROW_SECU_ACCS_C,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I
	FROM {{ ref('SrcTmpApptHlCondApprTera') }}
	WHERE {{ ref('SrcTmpApptHlCondApprTera') }}.REC_TYPE = 'I' OR {{ ref('SrcTmpApptHlCondApprTera') }}.REC_TYPE = 'U'
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptHlCondApprInsertDS