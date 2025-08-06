{{ config(materialized='incremental', alias='appt_attr_clas_assc', incremental_strategy='merge', tags=['LdApptAttrClasAsscFoinUpd']) }}

SELECT
	APPT_I
	APPT_ATTR_CLAS_C
	SRCE_SYST_C
	EFFT_D
	EXPY_D
	PROS_KEY_EXPY_I 
FROM {{ ref('TgtApptAttrClasAsscFoinUpdDS') }}