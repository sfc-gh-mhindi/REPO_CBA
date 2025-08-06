{{ config(materialized='view', tags=['DltAPPTAttrClasAsscFrmTMP_APPT_ATTR_CLAS_ASSC']) }}

WITH xfm_fhb_foin__outFOIN_upd AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS Expirydate,
		{{ ref('SrcTgtApptAttrClasAsscTera') }}.NEW_APPT_I AS APPT_I,
		{{ ref('SrcTgtApptAttrClasAsscTera') }}.OLD_APPT_ATTR_CLAS_C AS APPT_ATTR_CLAS_C,
		{{ ref('SrcTgtApptAttrClasAsscTera') }}.NEW_SRCE_SYST_C AS SRCE_SYST_C,
		{{ ref('SrcTgtApptAttrClasAsscTera') }}.OLD_EFFT_D AS EFFT_D,
		Expirydate AS EXPY_D,
		REFR_PK AS PROS_KEY_EXPY_I
	FROM {{ ref('SrcTgtApptAttrClasAsscTera') }}
	WHERE {{ ref('SrcTgtApptAttrClasAsscTera') }}.REC_TYPE_I = 'U' OR {{ ref('SrcTgtApptAttrClasAsscTera') }}.REC_TYPE_I = 'E'
)

SELECT * FROM xfm_fhb_foin__outFOIN_upd