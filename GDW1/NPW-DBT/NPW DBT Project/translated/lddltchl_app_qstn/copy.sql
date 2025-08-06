{{ config(materialized='view', tags=['LdDltChl_App_Qstn']) }}

WITH Copy AS (
	SELECT
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.NEW_APPT_I AS APPT_I,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.NEW_QSTN_C AS QSTN_C,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.NEW_RESP_C AS RESP_C,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.NEW_RESP_CMMT_X AS RESP_CMMT_X,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.NEW_PATY_I AS PATY_I,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.NEW_ROW_SECU_ACCS_C AS ROW_SECU_ACCS_C,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.OLD_APPT_I AS APPT_I,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.OLD_QSTN_C AS QSTN_C,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.OLD_RESP_C AS RESP_C,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.OLD_RESP_CMMT_X AS RESP_CMMT_X,
		{{ ref('Src_Tera_APPT_PDCT_COND') }}.OLD_PATY_I AS PATY_I
	FROM {{ ref('Src_Tera_APPT_PDCT_COND') }}
)

SELECT * FROM Copy