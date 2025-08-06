{{ config(materialized='view', tags=['XfmFaEnvisionEventFrmExt']) }}

WITH XfmBusinessRules__OutFAEnvEvntRejectsDS AS (
	SELECT
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((ModOut.CREATED_BY_STAFF_NUMBER)) THEN (ModOut.CREATED_BY_STAFF_NUMBER) ELSE ""))) = 0) then 'Y' else 'N',
		IFF(LEN(TRIM(IFF({{ ref('ModEvntActvTypeC') }}.CREATED_BY_STAFF_NUMBER IS NOT NULL, {{ ref('ModEvntActvTypeC') }}.CREATED_BY_STAFF_NUMBER, ''))) = 0, 'Y', 'N') AS svCreatedStaffNull,
		-- *SRC*: \(20)if (Len(trim(( IF IsNotNull((ModOut.FA_ENV_EVNT_CAT_ID)) THEN (ModOut.FA_ENV_EVNT_CAT_ID) ELSE ""))) = 0) then DEFAULT_NULL_VALUE else ModOut.EVNT_ACTV_TYPE_C,
		IFF(LEN(TRIM(IFF({{ ref('ModEvntActvTypeC') }}.FA_ENV_EVNT_CAT_ID IS NOT NULL, {{ ref('ModEvntActvTypeC') }}.FA_ENV_EVNT_CAT_ID, ''))) = 0, DEFAULT_NULL_VALUE, {{ ref('ModEvntActvTypeC') }}.EVNT_ACTV_TYPE_C) AS svEvntActvTypeC,
		-- *SRC*: \(20)If (Len(Trim(( IF IsNotNull((ModOut.CREATED_DATE)) THEN (ModOut.CREATED_DATE) ELSE ""))) = 0) then 'Y' else 'N',
		IFF(LEN(TRIM(IFF({{ ref('ModEvntActvTypeC') }}.CREATED_DATE IS NOT NULL, {{ ref('ModEvntActvTypeC') }}.CREATED_DATE, ''))) = 0, 'Y', 'N') AS CratDateIsNull,
		-- *SRC*: \(20)If CratDateIsNull = 'Y' then 'N' else ( if IsValid('date', StringToDate(trim(ModOut.CREATED_DATE)[1, 8], '%yyyy%mm%dd')) Then 'N' Else 'Y'),
		IFF(CratDateIsNull = 'Y', 'N', IFF(ISVALID('date', STRINGTODATE(SUBSTRING(TRIM({{ ref('ModEvntActvTypeC') }}.CREATED_DATE), 1, 8), '%yyyy%mm%dd')), 'N', 'Y')) AS ErrorCratDate,
		-- *SRC*: \(20)If CratDateIsNull = 'Y' then 'N' else ( if IsValid('time', StringToTime(trim(ModOut.CREATED_DATE)[9, 6], "%hh%nn%ss")) Then 'N' Else 'Y'),
		IFF(CratDateIsNull = 'Y', 'N', IFF(ISVALID('time', STRINGTOTIME(SUBSTRING(TRIM({{ ref('ModEvntActvTypeC') }}.CREATED_DATE), 9, 6), '%hh%nn%ss')), 'N', 'Y')) AS ErrorCratTime,
		-- *SRC*: \(20)If svEvntActvTypeC = '9999' then 'RPR6201' else '',
		IFF(svEvntActvTypeC = '9999', 'RPR6201', '') AS svErrorCode,
		-- *SRC*: \(20)If (svCreatedStaffNull = 'Y' or ModOut.FA_ENV_EVNT_CAT_ID <> '1') then 'N' else  if svUndertaking <> ModOut.FA_UNDERTAKING_ID then 'Y' else 'N',
		IFF(svCreatedStaffNull = 'Y' OR {{ ref('ModEvntActvTypeC') }}.FA_ENV_EVNT_CAT_ID <> '1', 'N', IFF(svUndertaking <> {{ ref('ModEvntActvTypeC') }}.FA_UNDERTAKING_ID, 'Y', 'N')) AS svIntGrupEmplF,
		-- *SRC*: \(20)If svIntGrupEmplF = 'Y' then ModOut.FA_UNDERTAKING_ID else svUndertaking,
		IFF(svIntGrupEmplF = 'Y', {{ ref('ModEvntActvTypeC') }}.FA_UNDERTAKING_ID, svUndertaking) AS svUndertaking,
		-- *SRC*: \(20)If (svCreatedStaffNull = 'Y') then 'N' else 'Y',
		IFF(svCreatedStaffNull = 'Y', 'N', 'Y') AS svEvntEmplF,
		FA_ENVISION_EVENT_ID,
		FA_UNDERTAKING_ID,
		{{ ref('ModEvntActvTypeC') }}.FA_ENV_EVNT_CAT_ID AS FA_ENVISION_EVENT_CAT_ID,
		CREATED_DATE,
		CREATED_BY_STAFF_NUMBER,
		COIN_REQUEST_ID,
		ETL_PROCESS_DT AS ETL_D,
		ORIG_ETL_D,
		svErrorCode AS EROR_C
	FROM {{ ref('ModEvntActvTypeC') }}
	WHERE svErrorCode <> ''
)

SELECT * FROM XfmBusinessRules__OutFAEnvEvntRejectsDS