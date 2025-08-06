{{ config(materialized='view', tags=['DltINT_GRUP_EMPLFrmTMP_FA_ENV_EVNT']) }}

WITH XfmCheckDeltaAction__OutFAEnvEvntDeltaRejectsDS AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS ExpiryDate,
		1 AS INSERT,
		3 AS UPDATE,
		-- *SRC*: \(20)If DSLink84.change_code = INSERT then 'RPR6202' else '',
		IFF({{ ref('Join_82') }}.change_code = INSERT, 'RPR6202', '') AS svErrorCode,
		{{ ref('Join_82') }}.FA_ENV_EVNT_ID AS FA_ENVISION_EVENT_ID,
		{{ ref('Join_82') }}.FA_UTAK_ID AS FA_UNDERTAKING_ID,
		{{ ref('Join_82') }}.FA_ENV_EVNT_CAT_ID AS FA_ENVISION_EVENT_CAT_ID,
		{{ ref('Join_82') }}.CRAT_DATE AS CREATED_DATE,
		{{ ref('Join_82') }}.CRAT_BY_STAF_NUM AS CREATED_BY_STAFF_NUMBER,
		{{ ref('Join_82') }}.COIN_REQ_ID AS COIN_REQUEST_ID,
		ETL_PROCESS_DT AS ETL_D,
		ORIG_ETL_D,
		svErrorCode AS EROR_C
	FROM {{ ref('Join_82') }}
	WHERE {{ ref('Join_82') }}.change_code = INSERT
)

SELECT * FROM XfmCheckDeltaAction__OutFAEnvEvntDeltaRejectsDS