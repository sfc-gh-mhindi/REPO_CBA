{{ config(materialized='view', tags=['DltAPPTAttrClasAsscFrmTMP_APPT_ATTR_CLAS_ASSC']) }}

WITH xfm_fhb_foin__outFOIN_ins AS (
	SELECT
		-- *SRC*: DateFromDaysSince(-1, StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd')),
		DATEFROMDAYSSINCE(-1, STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd')) AS Expirydate,
		{{ ref('SrcTgtApptAttrClasAsscTera') }}.NEW_APPT_I AS APPT_I,
		'FOIN' AS APPT_ATTR_CLAS_C,
		{{ ref('SrcTgtApptAttrClasAsscTera') }}.NEW_APPT_ATTR_CLAS_VALU_C AS APPT_ATTR_CLAS_VALU_C,
		-- *SRC*: StringToDate(ETL_PROCESS_DT, '%yyyy%mm%dd'),
		STRINGTODATE(ETL_PROCESS_DT, '%yyyy%mm%dd') AS EFFT_D,
		{{ ref('SrcTgtApptAttrClasAsscTera') }}.NEW_SRCE_SYST_C AS SRCE_SYST_C,
		-- *SRC*: StringToDate('9999-12-31', '%yyyy-%mm-%dd'),
		STRINGTODATE('9999-12-31', '%yyyy-%mm-%dd') AS EXPY_D,
		REFR_PK AS PROS_KEY_EFFT_I,
		-- *SRC*: SetNull(),
		SETNULL() AS PROS_KEY_EXPY_I,
		'0' AS ROW_SECU_ACCS_C
	FROM {{ ref('SrcTgtApptAttrClasAsscTera') }}
	WHERE {{ ref('SrcTgtApptAttrClasAsscTera') }}.REC_TYPE_I = 'I' OR {{ ref('SrcTgtApptAttrClasAsscTera') }}.REC_TYPE_I = 'U'
)

SELECT * FROM xfm_fhb_foin__outFOIN_ins