{{ config(materialized='view', tags=['DltAPPT_ASET_SETL_LOCNFrmTMP_APPT_ASET_SETL_LOCN']) }}

WITH XfmCheckDeltaAction__OutTgtApptAsetSetlLocnInsertDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		{{ ref('JoinAll') }}.NEW_APPT_I AS APPT_I,
		{{ ref('JoinAll') }}.NEW_ASET_I AS ASET_I,
		'CSE' AS SRCE_SYST_C,
		{{ ref('JoinAll') }}.NEW_FRWD_DOCU_C AS FRWD_DOCU_C,
		{{ ref('JoinAll') }}.NEW_SETL_LOCN_X AS SETL_LOCN_X,
		{{ ref('JoinAll') }}.NEW_SETL_CMMT_X AS SETL_CMMT_X,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS efft_d,
		-- *SRC*: StringToDate('9999-12-31', '%yyyy-%mm-%dd'),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS expy_d,
		REFR_PK AS pros_key_efft_i,
		-- *SRC*: setnull(),
		SETNULL() AS pros_key_expy_i
	FROM {{ ref('JoinAll') }}
	WHERE {{ ref('JoinAll') }}.change_code = INSERT OR {{ ref('JoinAll') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptAsetSetlLocnInsertDS