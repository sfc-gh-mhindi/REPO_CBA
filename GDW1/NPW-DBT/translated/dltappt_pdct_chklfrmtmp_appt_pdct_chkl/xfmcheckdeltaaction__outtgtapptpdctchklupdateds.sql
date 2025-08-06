{{ config(materialized='view', tags=['DltAPPT_PDCT_CHKLFrmTMP_APPT_PDCT_CHKL']) }}

WITH XfmCheckDeltaAction__OutTgtApptPdctChklUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		{{ ref('SrcTmpApptPdctChklTera') }}.NEW_APPT_PDCT_I AS APPT_PDCT_I,
		{{ ref('SrcTmpApptPdctChklTera') }}.NEW_CHKL_ITEM_C AS CHKL_ITEM_C,
		{{ ref('SrcTmpApptPdctChklTera') }}.NEW_SRCE_SYST_C AS SRCE_SYST_C,
		{{ ref('SrcTmpApptPdctChklTera') }}.OLD_EFFT_D AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('SrcTmpApptPdctChklTera') }}
	WHERE {{ ref('SrcTmpApptPdctChklTera') }}.INSERT_UPDATE_FLAG = 'U'
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptPdctChklUpdateDS