{{ config(materialized='incremental', alias='appt_attr_clas_assc', incremental_strategy='append', tags=['LdApptAttrClasAsscIns']) }}

SELECT
	APPT_I
	APPT_ATTR_CLAS_C
	APPT_ATTR_CLAS_VALU_C
	EFFT_D
	SRCE_SYST_C
	EXPY_D
	PROS_KEY_EFFT_I
	PROS_KEY_EXPY_I
	ROW_SECU_ACCS_C 
FROM {{ ref('TgtApptAttrClasAsscInsDS') }}