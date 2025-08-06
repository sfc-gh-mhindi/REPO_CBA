{{ config(materialized='view', tags=['XfmFaEnvisionEventFrmExt']) }}

WITH XfmBusinessRules__OutErrorCreateadTimeSeq AS (
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
		{{ ref('ModEvntActvTypeC') }}.FA_ENVISION_EVENT_ID AS SRCE_KEY_I,
		'ConversionTimestamp' AS CONV_M,
		'SRCTRMCHECK' AS CONV_MAP_RULE_M,
		'N/A' AS TRSF_TABL_M,
		{{ ref('ModEvntActvTypeC') }}.CREATED_DATE AS VALU_CHNG_BFOR_X,
		' ' AS VALU_CHNG_AFTR_X,
		-- *SRC*: 'Job Name = ' : DSJobName,
		CONCAT('Job Name = ', DSJobName) AS TRSF_X,
		'EVNT_ACTL_T' AS TRSF_COLM_M
	FROM {{ ref('ModEvntActvTypeC') }}
	WHERE ErrorCratTime = 'Y'
)

SELECT * FROM XfmBusinessRules__OutErrorCreateadTimeSeq