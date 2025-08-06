{{ config(materialized='view', tags=['DltAPPT_PDCT_DOCU_DELY_INSSFrmTMP_APPT_PDCT_DOCU_DELY_INSS']) }}

WITH XfmCheckDeltaAction__OutTgtAPPT_PDCT_DOCU_DELY_INSSInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		{{ ref('Join') }}.NEW_APPT_PDCT_I AS APPT_PDCT_I,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		{{ ref('Join') }}.NEW_DOCU_DELY_METH_C AS DOCU_DELY_METH_C,
		{{ ref('Join') }}.NEW_PYAD_TYPE_C AS PYAD_TYPE_C,
		{{ ref('Join') }}.NEW_ADRS_LINE_1_X AS ADRS_LINE_1_X,
		{{ ref('Join') }}.NEW_ADRS_LINE_2_X AS ADRS_LINE_2_X,
		{{ ref('Join') }}.NEW_SURB_X AS SURB_X,
		{{ ref('Join') }}.NEW_PCOD_C AS PCOD_C,
		{{ ref('Join') }}.NEW_STAT_X AS STAT_X,
		{{ ref('Join') }}.NEW_ISO_CNTY_C AS ISO_CNTY_C,
		{{ ref('Join') }}.NEW_BRCH_N AS BRCH_N,
		-- *SRC*: StringToDate('9999-12-31', '%yyyy-%mm-%dd'),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		REFR_PK_MIRROR AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: SetNull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('Join') }}
	WHERE {{ ref('Join') }}.change_code = INSERT OR {{ ref('Join') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtAPPT_PDCT_DOCU_DELY_INSSInsertDS