{{ config(materialized='view', tags=['DltASET_ADRSFrmTMP_ASET_ADRS']) }}

WITH XfmCheckDeltaAction__OutTgtApptFeatInsertDS AS (
	SELECT
		{{ ref('SrcTmpAsetAdrsTera') }}.NEW_ASET_I AS ASET_I,
		{{ ref('SrcTmpAsetAdrsTera') }}.NEW_ADRS_I AS ADRS_I,
		'CSE' AS SRCE_SYST_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		-- *SRC*: StringToDate('9999-12-31', '%yyyy-%mm-%dd'),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: setnull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		-- *SRC*: setnull(),
		SETNULL() AS EROR_SEQN_I
	FROM {{ ref('SrcTmpAsetAdrsTera') }}
	WHERE 
)

SELECT * FROM XfmCheckDeltaAction__OutTgtApptFeatInsertDS