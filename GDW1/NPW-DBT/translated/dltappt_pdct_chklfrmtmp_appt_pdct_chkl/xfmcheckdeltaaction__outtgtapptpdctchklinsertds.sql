{{ config(materialized='view', tags=['DltAPPT_PDCT_CHKLFrmTMP_APPT_PDCT_CHKL']) }}

WITH XfmCheckDeltaAction__OutTgtApptPdctChklInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		{{ ref('SrcTmpApptPdctChklTera') }}.NEW_APPT_PDCT_I AS APPT_PDCT_I,
		{{ ref('SrcTmpApptPdctChklTera') }}.NEW_CHKL_ITEM_C AS CHKL_ITEM_C,
		{{ ref('SrcTmpApptPdctChklTera') }}.NEW_STUS_D AS STUS_D,
		{{ ref('SrcTmpApptPdctChklTera') }}.NEW_STUS_C AS STUS_C,
		{{ ref('SrcTmpApptPdctChklTera') }}.NEW_SRCE_SYST_C AS SRCE_SYST_C,
		{{ ref('SrcTmpApptPdctChklTera') }}.NEW_CHKL_ITEM_X AS CHKL_ITEM_X,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		'9999-12-31' AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I
	FROM {{ ref('SrcTmpApptPdctChklTera') }}
	WHERE {{ ref('SrcTmpApptPdctChklTera') }}.INSERT_UPDATE_FLAG = 'I' OR {{ ref('SrcTmpApptPdctChklTera') }}.INSERT_UPDATE_FLAG = 'U'
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptPdctChklInsertDS