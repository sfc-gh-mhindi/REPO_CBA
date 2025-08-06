{{ config(materialized='view', tags=['DltAPPT_PDCT_DOCU_DELY_INSSFrmTMP_APPT_PDCT_DOCU_DELY_INSS']) }}

WITH XfmCheckDeltaAction__OutTgtDeptApptUpdateDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		{{ ref('Join') }}.NEW_APPT_PDCT_I AS APPT_PDCT_I,
		{{ ref('Join') }}.OLD_DOCU_DELY_METH_C AS DOCU_DELY_METH_C,
		-- *SRC*: ( IF IsNotNull((OutJoin.OLD_EFFT_D)) THEN (OutJoin.OLD_EFFT_D) ELSE ""),
		IFF({{ ref('Join') }}.OLD_EFFT_D IS NOT NULL, {{ ref('Join') }}.OLD_EFFT_D, '') AS EFFT_D,
		ExpiryDate AS EXPY_D,
		REFR_PK_MIRROR AS PROS_KEY_EXPY_I
	FROM {{ ref('Join') }}
	WHERE {{ ref('Join') }}.change_code = UPDATE
)

SELECT * FROM XfmCheckDeltaAction__OutTgtDeptApptUpdateDS