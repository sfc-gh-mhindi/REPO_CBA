{{ config(materialized='view', tags=['DltAPPTAttrClasAsscFrmTMP_APPT_ATTR_CLAS_ASSC']) }}

WITH xfm_fhb_foin__outFHBins AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS Expirydate,
		{{ ref('SrcTgtApptAttrClasAsscTera') }}.NEW_APPT_I AS APPT_I,
		{{ ref('SrcTgtApptAttrClasAsscTera') }}.NEW_APPT_ATTR_CLAS_C AS APPT_ATTR_CLAS_C,
		{{ ref('SrcTgtApptAttrClasAsscTera') }}.NEW_APPT_ATTR_CLAS_VALU_C AS APPT_ATTR_CLAS_VALU_C,
		{{ ref('SrcTgtApptAttrClasAsscTera') }}.NEW_EFFT_D AS EFFT_D,
		{{ ref('SrcTgtApptAttrClasAsscTera') }}.NEW_SRCE_SYST_C AS SRCE_SYST_C,
		{{ ref('SrcTgtApptAttrClasAsscTera') }}.NEW_EXPY_D AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		{{ ref('SrcTgtApptAttrClasAsscTera') }}.NEW_PROS_KEY_EXPY_I AS PROS_KEY_EXPY_I,
		{{ ref('SrcTgtApptAttrClasAsscTera') }}.NEW_ROW_SECU_ACCS_C AS ROW_SECU_ACCS_C
	FROM {{ ref('SrcTgtApptAttrClasAsscTera') }}
	WHERE {{ ref('SrcTgtApptAttrClasAsscTera') }}.OLD_APPT_I IS NULL AND {{ ref('SrcTgtApptAttrClasAsscTera') }}.NEW_APPT_ATTR_CLAS_C = 'FHBF'
)

SELECT * FROM xfm_fhb_foin__outFHBins